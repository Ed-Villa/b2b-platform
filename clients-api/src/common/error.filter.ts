import {ArgumentsHost, Catch, ExceptionFilter, HttpException} from '@nestjs/common';
import {Response} from 'express';

@Catch()
export class ErrorFilter implements ExceptionFilter {
    catch(error: unknown, host: ArgumentsHost): void {
        const response = host.switchToHttp().getResponse<Response>();
        const status = error instanceof HttpException ? error.getStatus() : 500;
        const code = status === 400 ? 'INVALID_ARGUMENT' : status === 404 ? 'NOT_FOUND' : 'UNAVAILABLE';
        const message = status === 400 ? 'Valid clientId is required' : status === 404 ? 'Client or route not found' : 'Client lookup unavailable';
        response.status(status).json({code, message, correlationId: response.getHeader('X-Correlation-Id')});
    }
}