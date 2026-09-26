FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build && npm test && npm prune --omit=dev
FROM node:22-alpine
WORKDIR /app
COPY --from=build /app/node_modules node_modules
COPY --from=build /app/dist dist
USER node
EXPOSE 3000
CMD ["node", "dist/main.js"]
