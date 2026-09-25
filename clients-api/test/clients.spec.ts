import 'reflect-metadata';
import {Test} from '@nestjs/testing';
import {INestApplication, NotFoundException} from '@nestjs/common';
import request from 'supertest';
import {AppModule} from '../src/app.module';
import {ClientsService} from '../src/clients/clients.service';
import {MemoryClients} from '../src/clients/memory-clients.repository';
import {configure} from '../src/http';

describe('Clients', () => {
    let app: INestApplication;
    beforeAll(async () => {
        const module = await Test.createTestingModule({imports: [AppModule]}).compile();
        app = module.createNestApplication();
        configure(app);
        await app.init();
    });
    afterAll(async () => {
        await app.close();
    });
    it('queries repository and rejects missing clients', () => {
        const service = new ClientsService(new MemoryClients());
        expect(service.find('CLI-99821').market).toBe('MX');
        expect(() => service.find('missing')).toThrow(NotFoundException);
    });
    it('returns a client with correlation', async () => {
        const response = await request(app.getHttpServer()).get('/clients/CLI-99821').set('X-Correlation-Id', 'event-1').expect(200);
        expect(response.body).toMatchObject({
            clientId: 'CLI-99821',
            status: 'ACTIVE',
            segment: 'WHOLESALE',
            taxRegime: 'GENERAL',
            market: 'MX'
        });
        expect(response.headers['x-correlation-id']).toBe('event-1');
    });
    it.each([['/clients/missing', 404, 'NOT_FOUND'], ['/clients/bad%20id', 400, 'INVALID_ARGUMENT']])('returns errors for %s', async (path, status, code) => {
        const response = await request(app.getHttpServer()).get(String(path)).expect(Number(status));
        expect(response.body.code).toBe(code);
        expect(response.body.correlationId).toBeTruthy();
    });
    it('exposes health', async () => {
        await request(app.getHttpServer()).get('/health').expect(200, {status: 'ok'});
    });
});
