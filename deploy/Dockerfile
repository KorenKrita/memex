# memex server: Bun, the server's own sources, no npm dependencies. Built from the repository root:
#   docker build -f deploy/Dockerfile -t memex-server .
FROM oven/bun:1.4.2-slim
WORKDIR /app
COPY server/ server/
COPY client/ client/
COPY dryrun/dryrun.ts dryrun/dryrun.ts
COPY tsconfig.json package.json ./
USER bun
ENV MEMEX_HOST=0.0.0.0 MEMEX_PORT=8787
EXPOSE 8787
# main.ts migrates the schema, then serves the API and runs the worker loop; SIGTERM closes both.
CMD ["bun", "server/main.ts"]
