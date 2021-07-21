export type Snowflake = `${bigint}`
export type InviteType = 'leaves' | 'regular' | 'fake' | 'bonus'
export type PluginName = 'join' | 'leave' | 'joinDM'

export interface CountGuildInvites {
    leaves: number;
    regular: number;
    fake: number;
    bonus: number;
}

export interface GuildStorage {
    guildID: Snowflake;
    storageID: string;
    createdAt: number;
}

export interface GuildSubscription {
    id: string;
    expiresAt: Date;
    createdAt: Date;
    subLabel: string;
    guildsCount: number;
    patreonUserID: string;
    cancelled: boolean;
    subInvalidated: boolean;
}

export interface SubscriptionPayment {
    id: string;
    payerDiscordID: string;
    amount: number;
    createdAt: Date;
    type: string;
    transactionID: string;
    details: unknown;
    modDiscordID: string;
    signupID: string;
    payerEmail: string;
    payerDiscordUsername: string;
}

export interface GuildSettings {
    guildID: Snowflake;
    storageID: string;
    language: string;
    prefix: string;
    cmdChannel: string;
    fakeThreshold: number;
}

export interface PremiumStatus {
    guildID: Snowflake;
    isPremium: boolean;
    isTrial: boolean;
}

export interface GuildSubscriptionStatus {
    isPayPal: boolean;
    isCancelled: boolean;
}

export interface GuildAlert {
    id: number;
    guildID: Snowflake;
    inviteCount: number;
    channelID: string;
    message: string;
    type: string;
}

export interface GuildPlugin {
    guildID: Snowflake;
    pluginName: PluginName;
    pluginData: Record<string, unknown>;
}

export interface UserLeaderboardEntry {
    userID: string;
    invites: number;
    regular: number;
    leaves: number;
    bonus: number;
    fake: number;
}

export interface GuildMember {
    userID: string;
    guildID: Snowflake;
    storageID: string;
    fake: number;
    leaves: number;
    bonus: number;
    regular: number;
    
    notCreated: boolean;
    invites: number;
}

type GuildMemberEventType = 'join' | 'leave'
type GuildMemberEventJoinType = 'normal' | 'oauth' | 'vanity' | 'perm' | 'unknown'

export interface GuildMemberEvent {
    userID: string;
    guildID: Snowflake;
    eventType: GuildMemberEventType;
    eventDate: number | Date;
    joinType: GuildMemberEventJoinType;
    inviterID: string;
    inviteData: unknown;
    storageID: string;
    joinFake: boolean;
}

export interface TransactionData {
    subID: string;
    guildID: Snowflake;
}

export interface NewlyCancelledPayment {
    payerDiscordID: string;
    subID: string;
    paymentID: string;
    guildID: Snowflake;
    subLabel: string;
}
