FROM node:20-alpine

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src

# Legion requires a non-root user; the node image ships one called "node".
USER node

# Legion injects PORT (2567); 2567 is also the default here so the image runs the same locally.
ENV PORT=2567
EXPOSE 2567

# Run node directly (not via npm) so SIGTERM reaches the server and it can drain.
CMD ["node", "src/index.js"]
