export interface Client {
    clientId: string;
    name: string;
    status: 'ACTIVE' | 'BLOCKED';
    segment: 'WHOLESALE' | 'RETAIL';
    taxRegime: 'GENERAL' | 'SIMPLIFIED' | 'EXEMPT';
    market: 'MX' | 'CO' | 'PE';
}