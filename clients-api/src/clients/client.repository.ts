import {Client} from './client';

export abstract class ClientRepository {
    abstract find(id: string): Client | undefined;
}