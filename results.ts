export type InviteType = 'leaves' | 'regular' | 'fake' | 'bonus';

export interface CountGuildInvites {
    leaves: number;
    regular: number;
    fake: number;
    bonus: number;
};

export interface GuildStorage {
    guildID: string;
    storageID: string;
    createdAt: number;
};

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
    details: any;
    modDiscordID: string;
    signupID: string;
    payerEmail: string;
    payerDiscordUsername: string;
};

export interface GuildSettings {
    guildID: string;
    storageID: string;
    language: string;
    prefix: string;
    keepRanks: boolean;
    stackedRanks: boolean;
    cmdChannel: string;
    fakeThreshold: number;
};

export interface PremiumStatus {
    guildID: string;
    isPremium: boolean;
    isTrial: boolean;
};

export interface GuildSubscriptionStatus {
    isPayPal: boolean;
    isCancelled: boolean;
};

export interface GuildRank {
    guildID: string;
    roleID: string;
    inviteCount: number;
};

export interface GuildPlugin {
    guildID: string;
    pluginName: string;
    pluginData: any;
};

export interface UserLeaderboardEntry {
    userID: string;
    invites: number;
    regular: number;
    leaves: number;
    bonus: number;
    fake: number;
}
