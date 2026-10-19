FROM rust:1-slim AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/live-chat-rust /usr/local/bin/live-chat
ENV PORT=3000
EXPOSE 3000
CMD ["live-chat"]
