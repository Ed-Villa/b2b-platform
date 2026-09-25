import {Injectable} from '@nestjs/common';
import {Client} from './client';
import {ClientRepository} from './client.repository';

@Injectable()
export class MemoryClients extends ClientRepository {
    private readonly clients: ReadonlyMap<string, Client> = new Map([
        ['CLI-99821', {
            clientId: 'CLI-99821',
            name: 'Distribuidora Central',
            status: 'ACTIVE',
            segment: 'WHOLESALE',
            taxRegime: 'GENERAL',
            market: 'MX'
        }],
        ['CLI-MX-2', {
            clientId: 'CLI-MX-2',
            name: 'Cliente MX bloqueado',
            status: 'BLOCKED',
            segment: 'RETAIL',
            taxRegime: 'GENERAL',
            market: 'MX'
        }],
        ['CLI-CO-1', {
            clientId: 'CLI-CO-1',
            name: 'Distribuidora Colombia',
            status: 'ACTIVE',
            segment: 'WHOLESALE',
            taxRegime: 'SIMPLIFIED',
            market: 'CO'
        }],
        ['CLI-CO-2', {
            clientId: 'CLI-CO-2',
            name: 'Minorista Colombia',
            status: 'ACTIVE',
            segment: 'RETAIL',
            taxRegime: 'EXEMPT',
            market: 'CO'
        }],
        ['CLI-PE-1', {
            clientId: 'CLI-PE-1',
            name: 'Distribuidora Peru',
            status: 'ACTIVE',
            segment: 'WHOLESALE',
            taxRegime: 'GENERAL',
            market: 'PE'
        }],
        ['CLI-PE-2', {
            clientId: 'CLI-PE-2',
            name: 'Minorista Peru',
            status: 'ACTIVE',
            segment: 'RETAIL',
            taxRegime: 'EXEMPT',
            market: 'PE'
        }],
    ]);

    find(id: string): Client | undefined {
        const client = this.clients.get(id);
        return client ? {...client} : undefined;
    }
}