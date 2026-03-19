FROM denoland/deno:2.0.0
WORKDIR /app
COPY deno.json deno.lock ./
COPY src/ ./src/
COPY integration/app/ ./integration/app/
RUN deno install
EXPOSE 8080
CMD ["deno", "run", "-A", "integration/app/main.ts"]
