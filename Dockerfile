# Stage 1: Build
FROM rust:1.75 as builder

WORKDIR /build

# 安装 protobuf 编译器
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./

# 复制源代码
COPY proto ./proto
COPY src ./src
COPY config ./config

# 构建发布版本
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制二进制文件
COPY --from=builder /build/target/release/kafka-clickhouse-sync /app/

# 创建配置目录
RUN mkdir -p /app/config

# 设置环境变量
ENV RUST_LOG=info
ENV CONFIG_PATH=/app/config/config.toml

# 暴露监控端口（未来使用）
EXPOSE 9090

# 运行服务
CMD ["/app/kafka-clickhouse-sync"]
