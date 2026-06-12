FROM oven/bun:1
WORKDIR /app
COPY package.json bun.lock ./
COPY src/ ./src/
COPY integration/app/ ./integration/app/
RUN bun install --frozen-lockfile
EXPOSE 8080
CMD ["bun", "run", "integration/app/main.ts"]
