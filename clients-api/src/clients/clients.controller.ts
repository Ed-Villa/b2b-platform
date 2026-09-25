import {Controller, Get, Param} from '@nestjs/common';
import {Client} from './client';
import {ClientParams} from './client-params.dto';
import {ClientsService} from './clients.service';

@Controller('clients')
export class ClientsController {
    constructor(private readonly clients: ClientsService) {
    }

    @Get(':clientId') find(@Param() params: ClientParams): Client {
        return this.clients.find(params.clientId);
    }
}