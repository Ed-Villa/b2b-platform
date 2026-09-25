import 'reflect-metadata';
import {NestFactory} from '@nestjs/core';
import {AppModule} from './app.module';
import {configure} from './http';

async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule);
    configure(app);
    await app.listen(process.env.PORT ?? 3000, '0.0.0.0');
}

void bootstrap();