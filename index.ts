import PostgresHandler from "./postgres";
import RedisHandler from "./redis";

import { snakeCase } from 'change-case';
import { RedisOptions } from "ioredis";
import { PoolConfig } from "pg";
import Logger from "./logger";
import { CountGuildInvites, GuildPlugin, GuildRank, GuildSettings, GuildStorage, GuildSubscription, GuildSubscriptionStatus, PremiumStatus, SubscriptionPayment, InviteType, UserLeaderboardEntry, GuildMember, GuildMemberEvent, TransactionData, NewlyCancelledPayment } from "./results";

const formatPayment = (paymentRow: any): SubscriptionPayment => ({
    id: paymentRow.id,
    payerDiscordID: paymentRow.payer_discord_id,
    amount: paymentRow.amount,
    createdAt: paymentRow.created_at,
    type: paymentRow.type,
    transactionID: paymentRow.transaction_id,
    details: paymentRow.details,
    modDiscordID: paymentRow.mod_discord_id,
    signupID: paymentRow.signup_id,
    payerEmail: paymentRow.payer_email,
    payerDiscordUsername: paymentRow.payer_discord_username
});

const generateStorageID = () => [...Array(12)].map(()=>(~~(Math.random()*36)).toString(36)).join("");

export default class DatabaseHandler {

    public redis: RedisHandler;
    public postgres: PostgresHandler;

    constructor (redisConfig: RedisOptions, postgresConfig: PoolConfig, logger?: Logger) {

        this.redis = new RedisHandler(redisConfig, logger);
        this.postgres = new PostgresHandler(postgresConfig, logger);

    }

    connect () {
        return Promise.all([
            this.redis.connect,
            this.postgres.connect
        ]);
    }

    /**
     * Count the guild invites present in the latest storage
     */
    async countGuildInvites (guildID: string, currentStorageID: string): Promise<CountGuildInvites|null> {
        const guildStorages = await this.fetchGuildStorages(guildID);
        const previousStorageID = guildStorages
            .filter((storage) => storage.storageID !== currentStorageID)
            .sort((a, b) => b.createdAt - a.createdAt)[0]?.storageID;
        if (!previousStorageID) return null;
        const { rows } = await this.postgres.query(`
            SELECT
                guild_id,
                SUM(invites_regular) as invites_regular,
                SUM(invites_fake) as invites_fake,
                SUM(invites_bonus) as invites_bonus,
                SUM(invites_leaves) as invites_leaves
            FROM members
            WHERE guild_id = $1
            AND storage_id = $2
            GROUP BY 1;
        `, guildID, previousStorageID);
        return {
            regular: rows[0]?.invites_regular || 0,
            leaves: rows[0]?.invites_leaves || 0,
            bonus: rows[0]?.invites_bonus || 0,
            fake: rows[0]?.invites_fake || 0
        } as CountGuildInvites;
    }

    /**
     * Remove the guild invites by creating a new storage
     * and change the guild settings so it's the default one
     */
    async removeGuildInvites (guildID: string): Promise<string> {
        const { rows } = await this.postgres.query(`
            UPDATE guilds
            SET guild_storage_id = $1
            WHERE guild_id = $2
            RETURNING guild_storage_id;
        `, generateStorageID(), guildID);
        const newStorageID = rows[0].guild_storage_id;

        await this.redis.setHash(`guild_${guildID}`, {
            storageID: newStorageID
        });

        await this.postgres.query(`
            INSERT INTO guild_storages
            (guild_id, storage_id, created_at) VALUES
            ($1, $2, $3);
        `, guildID, newStorageID, new Date().toISOString());

        return newStorageID;
    }

    /**
     * Fetches all the guild storages
     */
    async fetchGuildStorages (guildID: string): Promise<GuildStorage[]> {
        const { rows } = await this.postgres.query(`
            SELECT *
            FROM guild_storages
            WHERE guild_id = $1;
        `, guildID);
        return rows.map((row: any) => ({
            guildID: row.guild_id,
            storageID: row.storage_id,
            createdAt: new Date(row.created_at).getTime()
        })) as GuildStorage[];
    }

    /**
     * Restore a guild storage by chnging the guild settings
     */
    async restoreGuildStorage ({ guildID, storageID }: { guildID: string, storageID: string }): Promise<void> {
        const redisUpdatePromise: Promise<any> = this.redis.setHash(`guild_${guildID}`, {
            storageID
        });
        const postgresUpdatePromise = this.postgres.query(`
            UPDATE guilds
            SET guild_storage_id = $1
            WHERE guild_id = $2
            RETURNING guild_storage_id;
        `, storageID, guildID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Restore the guild invites by changing back the storage id
     */
    async restoreGuildInvites (guildID: string, currentStorageID: string): Promise<void> {
        const guildStorages = await this.fetchGuildStorages(guildID);
        const latestStorageID = guildStorages
            .filter((storage) => storage.storageID !== currentStorageID)
            .sort((a, b) => b.createdAt - a.createdAt)[0]?.storageID;
        await this.restoreGuildStorage({
            guildID,
            storageID: latestStorageID
        });
    }

    /**
     * Fetches the guild subscriptions
     */
    async fetchGuildSubscriptions (guildID: string): Promise<GuildSubscription[]> {
        const redisData = await this.redis.getString(`guild_subscriptions_${guildID}`, true);
        if (redisData) return redisData as GuildSubscription[];

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM subscriptions
            INNER JOIN guilds_subscriptions ON guilds_subscriptions.sub_id = subscriptions.id
            WHERE guilds_subscriptions.guild_id = $1;
        `, guildID);

        const formattedGuildSubscriptions: GuildSubscription[] = rows.map((row) => ({
            id: row.id,
            expiresAt: row.expires_at,
            createdAt: row.created_at,
            subLabel: row.sub_label,
            guildsCount: row.guilds_count,
            patreonUserID: row.patreon_user_id,
            cancelled: row.cancelled,
            subInvalidated: row.sub_invalidated
        }));
        
        this.redis.setString(`guild_subscriptions_${guildID}`, JSON.stringify(formattedGuildSubscriptions));

        return formattedGuildSubscriptions;
    }

    /**
     * Create a payment for the given subscription
     */
    async createSubscriptionPayment (subID: string, { payerDiscordID, payerDiscordUsername, payerEmail, amount, createdAt = new Date(), type, transactionID, details = {}, signupID, modDiscordID }: Partial<SubscriptionPayment>): Promise<SubscriptionPayment> {
        const { rows } = await this.postgres.query(`
            INSERT INTO payments
            (payer_discord_id, payer_discord_username, payer_email, amount, created_at, type, transaction_id, details, signup_id, mod_discord_id) VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *;
        `, payerDiscordID, payerDiscordUsername, payerEmail, amount, createdAt.toISOString(), type, transactionID, JSON.stringify(details), signupID, modDiscordID);
        const paymentID = rows[0].id;
        await this.postgres.query(`
            INSERT INTO subscriptions_payments
            (sub_id, payment_id) VALUES
            ($1, $2);
        `, subID, paymentID);
        return formatPayment(rows[0]);
    }

    /**
     * Create a subscription for the given guild
     */
    async createGuildSubscription (guildID: string, { expiresAt = new Date(), createdAt = new Date(), subLabel, guildsCount = 1, patreonUserID }: Partial<GuildSubscription>) {
        const { rows } = await this.postgres.query(`
            INSERT INTO subscriptions
            (expires_at, created_at, sub_label, guilds_count, patreon_user_id) VALUES
            ($1, $2, $3, $4, $5)
            RETURNING id;
        `, expiresAt.toISOString(), createdAt.toISOString(), subLabel, guildsCount, patreonUserID);
        const subID = rows[0].id;
        await this.postgres.query(`
            INSERT INTO guilds_subscriptions
            (sub_id, guild_id) VALUES
            ($1, $2);
        `, subID, guildID);
        const guildSubscriptions = await this.redis.getString(`guild_subscriptions_${guildID}`, true) as GuildSubscription[];
        const newSubscription = {
            expiresAt,
            createdAt,
            subLabel,
            guildsCount,
            patreonUserID,
            id: subID
        };
        const newGuildSubscriptions = [
            ...guildSubscriptions,
            newSubscription
        ];
        await this.redis.setString(`guild_subscriptions_${guildID}`, JSON.stringify(newGuildSubscriptions));
        return newSubscription as GuildSubscription;
    }

    /**
     * Update a property of the guild subscription
     */
    updateGuildSubscription (subID: string, guildID: string, settingName: keyof GuildSubscription, newSettingValue: any): Promise<void> {
        if (!["expires_at", "created_at", "sub_label", "guilds_count", "patreon_user_id", "cancelled", "sub_invalidated"].includes(snakeCase(settingName))) throw new Error("unknown_guild_setting");
        const redisUpdatePromise = this.redis.getString(`guild_subscriptions_${guildID}`, true).then((guildSubscriptions) => {
            if (guildSubscriptions) {
                const guildSubscription = (guildSubscriptions as GuildSubscription[]).find((sub) => sub.id === subID)!;
                const newSubscription: GuildSubscription = { ...guildSubscription, ...{ [settingName]: newSettingValue } };
                const newGuildSubscriptions = [
                    ...(guildSubscriptions as GuildSubscription[]).filter((sub) => sub.id !== subID),
                    newSubscription
                ];
                return this.redis.setString(`guild_subscriptions_${guildID}`, JSON.stringify(newGuildSubscriptions));
            }
        });
        const postgresUpdatePromise = this.postgres.query(`
            UPDATE subscriptions
            SET ${snakeCase(settingName)} = $1
            WHERE id = $2;
        `, newSettingValue, subID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Get the subscription status of a guild (to check whether it's maintained by PayPal and cancelled)
     */
    async fetchGuildSubscriptionStatus (guildID: string): Promise<GuildSubscriptionStatus> {
        const guildSubscriptions = await this.fetchGuildSubscriptions(guildID);
        const payments = (await Promise.all(guildSubscriptions.map((sub) => this.fetchSubscriptionPayments(sub.id)))).flat();
        const isPayPal = payments.some((p) => p.type.startsWith("paypal_dash_signup"));
        const isCancelled = payments.filter((p) => p.type.startsWith("paypal_dash_signup")).length > payments.filter((p) => p.type.startsWith("paypal_dash_cancel")).length;
        return {
            isPayPal,
            isCancelled
        } as GuildSubscriptionStatus;
    }

    /**
     * Check the premium status for the given guild ids
     */
    async fetchGuildsPremiumStatuses (guildsID: string[]): Promise<PremiumStatus[]> {
        const guildsSubscriptions = await Promise.all(guildsID.map((g) => this.fetchGuildSubscriptions(g)));
        return guildsSubscriptions.map((subscriptions, index) => ({
            guildID: guildsID[index],
            isPremium: subscriptions.some((sub) => new Date(sub.expiresAt).getTime() > Date.now()),
            isTrial: subscriptions.some((sub) => sub.subLabel === "Trial Version" && new Date(sub.expiresAt).getTime() > Date.now())
        })) as PremiumStatus[];
    }
    
    /**
     * Get the guild settings for the given guild id
     */
    async fetchGuildSettings (guildID: string): Promise<GuildSettings> {
        const redisData = await this.redis.getHashFields(`guild_${guildID}`);
        if (redisData?.guildID) return {
            guildID: redisData.guildID,
            storageID: redisData.storageID,
            language: redisData.language,
            prefix: redisData.prefix,
            keepRanks: redisData.keepRanks === "true",
            stackedRanks: redisData.stackedRanks === "true",
            cmdChannel: redisData.cmdChannel,
            fakeThreshold: parseInt(redisData.fakeThreshold)
        } as GuildSettings;

        let { rows } = await this.postgres.query(`
            SELECT *
            FROM guilds
            WHERE guild_id = $1;
        `, guildID);
        
        if (!rows[0]) {
            ({ rows } = await this.postgres.query(`
                INSERT INTO guilds
                (guild_id, guild_language, guild_prefix, guild_keep_ranks, guild_stacked_ranks, guild_cmd_channel, guild_fake_threshold, guild_storage_id) VALUES
                ($1, 'en-US', '+', false, false, null, null, $2)
                RETURNING guild_storage_id;
            `, guildID, generateStorageID()));
            await this.postgres.query(`
                INSERT INTO guild_storages
                (guild_id, storage_id, created_at) VALUES
                ($1, $2, $3);
            `, guildID, rows[0].guild_storage_id, new Date().toISOString());
        }

        const formattedGuildSettings: GuildSettings = {
            guildID: rows[0].guild_id,
            storageID: rows[0].guild_storage_id,
            language: rows[0].guild_language,
            prefix: rows[0].guild_prefix,
            keepRanks: rows[0].guild_keep_ranks,
            stackedRanks: rows[0].guild_stacked_ranks,
            cmdChannel: rows[0].guild_cmd_channel,
            fakeThreshold: rows[0].guild_fake_threshold
        };

        this.redis.setHash(`guild_${guildID}`, formattedGuildSettings);

        return formattedGuildSettings;
    }

    /**
     * Update a property of a guild settings
     */
    updateGuildSetting (guildID: string, settingName: keyof GuildSettings, newSettingValue: any): Promise<void> {
        if (!["language", "prefix", "cmd_channel", "fake_treshold", "keep_ranks", "stacked_ranks", "storage_id"].includes(snakeCase(settingName))) throw new Error("unknown_guild_setting");
        const redisUpdatePromise: Promise<any> = this.redis.setHash(`guild_${guildID}`, {
            [settingName]: newSettingValue
        });
        const postgresUpdatePromise = this.postgres.query(`
            UPDATE guilds
            SET guild_${snakeCase(settingName)} = $1
            WHERE guild_id = $2;
        `, newSettingValue, guildID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Add a new guild rank
     */
    addGuildRank (guildID: string, roleID: string, inviteCount: number): Promise<void> {
        const redisUpdatePromise = this.redis.getString(`guild_ranks_${guildID}`, true).then((ranks) => {
            if (!ranks) return;
            const newRanks = [
                ...(ranks as GuildRank[]),
                {
                    guildID,
                    roleID,
                    inviteCount
                }
            ];
            return this.redis.setString(`guild_ranks_${guildID}`, JSON.stringify(newRanks));
        });
        const postgresUpdatePromise = this.postgres.query(`
            INSERT INTO guild_ranks
            (guild_id, role_id, invite_count) VALUES
            ($1, $2, $3)
        `, guildID, roleID, inviteCount);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Remove an existing guild rank
     */
    async removeGuildRank (guildID: string, roleID: string): Promise<void> {
        const redisUpdatePromise = this.redis.getString(`guild_ranks_${guildID}`, true).then((ranks) => {
            if (!ranks) return;
            const newRanks = (ranks as GuildRank[]).filter((rank) => rank.roleID !== roleID);
            return this.redis.setString(`guild_ranks_${guildID}`, JSON.stringify(newRanks));
        });
        const postgresUpdatePromise = this.postgres.query(`
            DELETE FROM guild_ranks
            WHERE role_id = $1
            AND guild_id = $2;
        `, roleID, guildID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Get the ranks of a guild
     */
    async fetchGuildRanks (guildID: string): Promise<GuildRank[]> {
        const redisData = await this.redis.getString(`guild_ranks_${guildID}`, true);
        if (redisData) return redisData as GuildRank[];

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM guild_ranks
            WHERE guild_id = $1;
        `, guildID);
        const formattedRanks: GuildRank[] = rows.map((row) => ({
            guildID: row.guild_id,
            roleID: row.role_id,
            inviteCount: row.invite_count
        }));
        this.redis.setString(`guild_ranks_${guildID}`, JSON.stringify(formattedRanks));
        return formattedRanks;
    }

    /**
     * Get the plugins of a guild
     */
    async fetchGuildPlugins (guildID: string): Promise<GuildPlugin[]> {
        const redisData = await this.redis.getString(`guild_plugins_${guildID}`, true);
        if (redisData) return redisData as GuildPlugin[];

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM guild_plugins
            WHERE guild_id = $1;
        `, guildID);
        const formattedPlugins: GuildPlugin[] = rows.map((row) => ({
            guildID: row.guild_id,
            pluginName: row.plugin_name,
            pluginData: row.plugin_data
        }));
        this.redis.setString(`guild_plugins_${guildID}`, JSON.stringify(formattedPlugins));
        return formattedPlugins;
    }

    /**
     * Update a guild plugin
     */
    updateGuildPlugin (guildID: string, pluginName: string, newPluginData: any): Promise<void> {
        const redisUpdatePromise = this.redis.getString(`guild_plugins_${guildID}`, true).then((data) => {
            let newFormattedPlugins = [...(data as GuildPlugin[])];
            newFormattedPlugins = newFormattedPlugins.filter((p) => p.pluginName !== pluginName);
            newFormattedPlugins.push({
                guildID,
                pluginName,
                pluginData: newPluginData
            });
            return this.redis.setString(`guild_plugins_${guildID}`, JSON.stringify(newFormattedPlugins));
        });
        const postgresUpdatePromise = this.postgres.query(`
            SELECT *
            FROM guild_plugins
            WHERE plugin_name = $1
            AND guild_id = $2;
        `, pluginName, guildID).then(({ rowCount }) => {
            // if the plugin exists
            if (rowCount > 0) {
                return this.postgres.query(`
                    UPDATE guild_plugins
                    SET plugin_data = $1
                    WHERE plugin_name = $2
                    AND guild_id = $3;
                `, JSON.stringify(newPluginData), pluginName, guildID);
            } else {
                return this.postgres.query(`
                    INSERT INTO guild_plugins
                    (plugin_data, plugin_name, guild_id) VALUES
                    ($1, $2, $3);
                `, JSON.stringify(newPluginData), pluginName, guildID);
            }
        });
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Add X invites to a member / the server
     */
    addInvites ({ userID, guildID, storageID, number, type }: { userID: string, guildID: string, storageID: string, number: number, type: InviteType}): Promise<void> {
        const redisUpdatePromise = this.redis.incrHashBy(`member_${userID}_${guildID}_${storageID}`, type, number);
        const postgresUpdatePromise = this.postgres.query(`
            UPDATE members
            SET invites_${type} = invites_${type} + $1
            WHERE user_id = $2
            AND guild_id = $3
            AND storage_id = $4;
        `, number, userID, guildID, storageID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Add invites to a server
     */
    addGuildInvites ({ usersID, guildID, storageID, number, type }: { usersID: string[], guildID: string, storageID: string, number: number, type: InviteType }): Promise<void> {
        const redisUpdates: Promise<any>[] = usersID.map((userID) => this.redis.incrHashBy(`member_${userID}_${guildID}_${storageID}`, type, number));
        const postgresUpdate = this.postgres.query(`
            UPDATE members
            SET invites_${type} = invites_${type} + $1
            WHERE guild_id = $2
            AND storage_id = $3;
        `, number, guildID, storageID);
        return Promise.all([ Promise.all(redisUpdates), postgresUpdate ]).then(() => {});
    }

    /**
     * Get the guild blacklisted users
     */
    async fetchGuildBlacklistedUsers (guildID: string): Promise<string[]> {
        const redisData = await this.redis.getString(`guild_blacklisted_${guildID}`, true);
        if (redisData) return redisData as string[];

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM guild_blacklisted_users
            WHERE guild_id = $1;
        `, guildID);

        const formattedBlacklistedUsers = rows.map((row) => row.user_id);

        this.redis.setString(`guild_blacklisted_${guildID}`, JSON.stringify(formattedBlacklistedUsers));
        return formattedBlacklistedUsers;
    }

    /**
     * Add a user to the guild blacklist
     */
    addGuildBlacklistedUser ({ guildID, userID }: { guildID: string, userID: string }): Promise<void> {
        const redisUpdatePromise = this.redis.getString(`guild_blacklisted_${guildID}`, true).then((blacklisted) => {
            if (!blacklisted) return;
            const newBlacklisted = [ ...(blacklisted as string[]), userID ];
            return this.redis.setString(`guild_blacklisted_${guildID}`, JSON.stringify(newBlacklisted));
        });
        const postgresUpdatePromise = this.postgres.query(`
            INSERT INTO guild_blacklisted_users
            (user_id, guild_id) VALUES
            ($1, $2);
        `, userID, guildID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Remove a user from the guild blacklist
     */
    removeGuildBlacklistedUser ({ guildID, userID }: { guildID: string, userID: string }): Promise<void> {
        const redisUpdatePromise = this.redis.getString(`guild_blacklisted_${guildID}`, true).then((blacklisted) => {
            if (!blacklisted) return;
            let newBlacklisted = [ ...(blacklisted as string[]) ];
            newBlacklisted = newBlacklisted.filter((id) => id !== userID);
            return this.redis.setString(`guild_blacklisted_${guildID}`, JSON.stringify(newBlacklisted));
        });
        const postgresUpdatePromise = this.postgres.query(`
            DELETE FROM guild_blacklisted_users
            WHERE user_id = $1
            AND guild_id = $2;
        `, userID, guildID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Get the guild leaderboard
     */
    async fetchGuildLeaderboard (guildID: string, storageID: string): Promise<UserLeaderboardEntry[]> {
        const redisData = await this.redis.getString(`guild_leaderboard_${guildID}`, true);
        if (redisData) return redisData as UserLeaderboardEntry[];

        const { rows } = await this.postgres.query(`
            SELECT user_id, user_id, invites_regular, invites_leaves, invites_bonus, invites_fake
            FROM members
            WHERE guild_id = $1
            AND storage_id = $2;
        `, guildID, storageID);

        const formattedMembers: UserLeaderboardEntry[] = rows.map((row) => ({
            guildID: row.guild_id,
            userID: row.user_id,
            invites: row.invites_regular + row.invites_bonus - row.invites_leaves - row.invites_fake,
            regular: row.invites_regular,
            leaves: row.invites_leaves,
            bonus: row.invites_bonus,
            fake: row.invites_fake
        }));

        this.redis.setString(`guild_leaderboard_${guildID}`, JSON.stringify(formattedMembers));
        this.redis.client.expire(`guild_leaderboard_${guildID}`, 10);

        return formattedMembers;
    }

    /**
     * Create a guild member (before updating it)
     */
    createGuildMember ({ userID, guildID, storageID }: { userID: string, guildID: string, storageID: string }): Promise<void> {
        const redisUpdatePromise: Promise<any> = this.redis.setHash(`member_${userID}_${guildID}_${storageID}`, {
            notCreated: false
        });
        const postgresUpdatePromise = this.postgres.query(`
            INSERT INTO members
            (
                guild_id, user_id, storage_id,
                invites_fake, invites_leaves, invites_bonus, invites_regular
            ) VALUES
            (
                $1, $2, $3,
                0, 0, 0, 0
            )
            RETURNING *;
        `, guildID, userID, storageID);
        return Promise.all([ redisUpdatePromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Get a guild member
     */
    async fetchGuildMember ({ userID, guildID, storageID }: { userID: string, guildID: string, storageID: string }): Promise<GuildMember> {
        const redisData = await this.redis.getHashFields(`member_${userID}_${guildID}_${storageID}`);
        if (redisData?.userID) return {
            userID: redisData.userID,
            guildID: redisData.guildID,
            storageID: redisData.storageID,
            fake: parseInt(redisData.fake),
            leaves: parseInt(redisData.leaves),
            bonus: parseInt(redisData.bonus),
            regular: parseInt(redisData.regular),
            
            notCreated: redisData.notCreated === "true",
            invites: parseInt(redisData.regular) + parseInt(redisData.bonus) - parseInt(redisData.leaves) - parseInt(redisData.fake)
        };

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM members
            WHERE guild_id = $1
            AND user_id = $2
            AND storage_id = $3;
        `, guildID, userID, storageID);

        const formattedMember= {
            userID: rows[0] ? rows[0].user_id : 0,
            guildID: rows[0] ? rows[0].guild_id : 0,
            storageID: rows[0] ? rows[0].storage_id : storageID,
            fake: rows[0] ? rows[0].invites_fake : 0,
            leaves: rows[0] ? rows[0].invites_leaves : 0,
            bonus: rows[0] ? rows[0].invites_bonus : 0,
            regular: rows[0] ? rows[0].invites_regular : 0,
            
            notCreated: !rows[0]
        };
        this.redis.setHash(`member_${userID}_${guildID}_${storageID}`, formattedMember);
        return {
            ...formattedMember,
            invites: formattedMember.regular + formattedMember.bonus - formattedMember.leaves - formattedMember.fake
        } as GuildMember;
    }

    /**
     * Get the member events (the events where they were invited and the events where they invited someone else)
     */
    async fetchGuildMemberEvents ({ userID, guildID }: { userID: string, guildID: string }): Promise<GuildMemberEvent[]> {
        const redisData = await this.redis.getString(`member_${userID}_${guildID}_events`, true);
        if (redisData) return redisData as GuildMemberEvent[];

        const { rows } = await this.postgres.query(`
            SELECT *
            FROM invited_member_events
            WHERE (user_id = $1
            OR inviter_user_id = $1)
            AND guild_id = $2;
        `, userID, guildID);

        const formattedEvents: GuildMemberEvent[] = rows.map((row) => ({
            userID: row.user_id,
            guildID: row.guild_id,
            eventType: row.event_type,
            eventDate: new Date(row.event_date).getTime(),
            joinType: row.join_type,
            inviterID: row.inviter_user_id,
            inviteData: row.invite_data,
            storageID: row.storage_id,
            joinFake: row.join_fake
        }));
        this.redis.setString(`member_${userID}_${guildID}_events`, JSON.stringify(formattedEvents));

        return formattedEvents;
    }

    /**
     * Get the payments of a subscription
     */
    async fetchSubscriptionPayments (subID: string): Promise<SubscriptionPayment[]> {
        const { rows } = await this.postgres.query(`
            SELECT *
            FROM payments p
            INNER JOIN subscriptions_payments sp ON sp.payment_id = p.id
            WHERE sp.sub_id = $1;
        `, subID);
        
        return rows.map((row) => formatPayment(row)) as SubscriptionPayment[];
    }

    /**
     * Insert a new event record in the database
     */
    async createGuildMemberEvent ({ userID, guildID, eventDate = new Date(), eventType, joinType, inviterID, inviteData, joinFake, storageID }: Partial<GuildMemberEvent>): Promise<void> {
        const redisUpdateUserPromise = this.redis.getString(`member_${userID}_${guildID}_events`, true).then((data) => {
            if (data) {
                const newData = [...(data as GuildMemberEvent[]), { userID, guildID, eventDate: (eventDate as Date).getTime(), eventType, joinType, inviterID, inviteData, joinFake, storageID }];
                return this.redis.setString(`member_${userID}_${guildID}_events`, JSON.stringify(newData));
            }
        });
        const redisUpdateInviterPromise = inviterID ? this.redis.getString(`member_${inviterID}_${guildID}_events`, true).then((data) => {
            if (data) {
                const newData = [...(data as GuildMemberEvent[]), { userID, guildID, eventDate: (eventDate as Date).getTime(), eventType, joinType, inviterID, inviteData, joinFake, storageID }];
                return this.redis.setString(`member_${inviterID}_${guildID}_events`, JSON.stringify(newData));
            }
        }) : null;
        const postgresUpdatePromise = this.postgres.query(`
            INSERT INTO invited_member_events
            (user_id, guild_id, event_date, event_type, join_type, inviter_user_id, invite_data, join_fake, storage_id) VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9);
        `, userID, guildID, (eventDate as Date).toISOString(), eventType, joinType, inviterID, inviteData, joinFake, storageID);
        return Promise.all([ redisUpdateUserPromise, redisUpdateInviterPromise, postgresUpdatePromise ]).then(() => {});
    }

    /**
     * Fetch the premium users
     */
    async fetchPremiumUserIDs (): Promise<string[]> {
        const { rows } = await this.postgres.query(`
            SELECT payer_discord_id
            FROM payments
            WHERE type = 'paypal_dash_pmnt_month'
            OR type = 'email_address_pmnt_month';
        `);
        return rows.map((row) => row.payer_discord_id) as string[];
    }

    /**
     * Fetch the premium guilds
     */
    async fetchPremiumGuildIDs (): Promise<string[]> {
        const { rows } = await this.postgres.query(`
            SELECT guild_id
            FROM guilds_subscriptions
        `);
        return rows.map((row) => row.guild_id) as string[];
    }

    /**
     * Fetch the data of a transaction
     */
    async fetchTransactionData (transactionID: string): Promise<TransactionData|null> {
        let { rows } = await this.postgres.query(`
            SELECT id
            FROM payments
            WHERE transaction_id = $1;
        `, transactionID);

        const paymentID = rows[0]?.id;
        if (!paymentID) return null;

        ({ rows } = await this.postgres.query(`
            SELECT sub_id
            FROM subscriptions_payments
            WHERE payment_id = $1;
        `, paymentID));

        const subID = rows[0]?.sub_id;
        if (!subID) return null;

        ({ rows } = await this.postgres.query(`
            SELECT guild_id
            FROM guilds_subscriptions
            WHERE sub_id = $1;
        `, subID));

        const guildID = rows[0]?.guild_id;
        if (!guildID) return null;

        return {
            subID,
            guildID
        };
    }

    /**
     * Mark a payment as already reminded
     */
    setPaymentRemindSent ({ paymentID, subID, success, kicked }: { paymentID: string, subID: string, success: boolean, kicked: boolean }): Promise<void> {
        return this.postgres.query(`
            INSERT INTO payments_reminds
            (last_payment_id, sub_id, success_sent, bot_kicked) VALUES
            ($1, $2, $3, $4);
        `, paymentID, subID, success, kicked).then(() => {});
    }

    /**
     * Fetch all the subscriptions that have not been paid (they have expired 3 days ago at least).
     * It also checks if a DM has already been sent to the member in charge of the subscription.
     */
    async fetchNewlyCancelledPayments (): Promise<NewlyCancelledPayment[]> {
        const { rows } = await this.postgres.query(`
            SELECT * FROM (
                SELECT distinct on (s.id) s.id as sub_id, p.id as payment_id, p.type, gs.guild_id, p.payer_discord_id, p.payer_discord_username, s.sub_label, s.expires_at, p.details
                FROM guilds_subscriptions gs
                INNER JOIN subscriptions s ON s.id = gs.sub_id
                INNER JOIN subscriptions_payments sp ON sp.sub_id = s.id
                INNER JOIN payments p ON p.id = sp.payment_id
                AND s.expires_at < now() - interval '3 days'
                AND s.expires_at > now() - interval '5 days'
                AND gs.guild_id NOT IN (
                    SELECT guild_id FROM guilds_subscriptions gs
                    INNER JOIN subscriptions s ON gs.sub_id = s.id
                    WHERE s.expires_at >= now()
                )
                ORDER BY s.id, p.created_at
            ) p_join WHERE payment_id NOT IN (
                SELECT last_payment_id FROM payments_reminds
            )
        `);
        return rows.map((row) => ({
            payerDiscordID: row.payer_discord_id,
            subID: row.sub_id,
            paymentID: row.payment_id,
            guildID: row.guild_id,
            subLabel: row.sub_label
        })) as NewlyCancelledPayment[];
    }

};
