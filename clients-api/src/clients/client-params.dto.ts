import {Matches} from 'class-validator';

export class ClientParams {
    @Matches(/^[A-Za-z0-9_-]{1,128}$/)
    clientId!: string;
}