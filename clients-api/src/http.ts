import {INestApplication, ValidationPipe} from '@nestjs/common';
import {NextFunction, Request, Response} from 'express';
import {randomUUID} from 'node:crypto';
import {ErrorFilter} from './common/error.filter';

export function configure(app: INestApplication): void {
    app.use((req: Request, res: Response, next: NextFunction) => {
        const incoming = req.header('X-Correlation-Id');
        const correlationId = incoming && /^[A-Za-z0-9_-]{1,128}$/.test(incoming) ? incoming : randomUUID();
        res.setHeader('X-Correlation-Id', correlationId);
        const start = Date.now();
        res.on('finish', () => console.log(JSON.stringify({
            transition: 'http_completed',
            correlationId,
            status: res.statusCode,
            durationMs: Date.now() - start
        })));
        next();
    });
    app.useGlobalPipes(new ValidationPipe({transform: true, whitelist: true, forbidNonWhitelisted: true}));
    app.useGlobalFilters(new ErrorFilter());
    app.enableShutdownHooks();
}