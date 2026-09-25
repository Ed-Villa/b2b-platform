import {Module} from '@nestjs/common';
import {ClientRepository} from './clients/client.repository';
import {MemoryClients} from './clients/memory-clients.repository';
import {ClientsService} from './clients/clients.service';
import {ClientsController} from './clients/clients.controller';
import {HealthController} from './health/health.controller';

@Module({
    controllers: [ClientsController, HealthController],
    providers: [ClientsService, {provide: ClientRepository, useClass: MemoryClients}],
})
export class AppModule {
}