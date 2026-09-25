import {Injectable, NotFoundException} from '@nestjs/common';
import {Client} from './client';
import {ClientRepository} from './client.repository';

@Injectable()
export class ClientsService {
    constructor(private readonly repository: ClientRepository) {
    }

    find(id: string): Client {
        const client = this.repository.find(id);
        if (!client) throw new NotFoundException('Client not found');
        return client;
    }
}