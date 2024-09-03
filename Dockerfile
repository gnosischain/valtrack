ARG GO_VERSION=1.22.5

# Stage 1: Dependency management and build
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies and verify modules
RUN go mod download && go mod verify

# Copy the rest of the application source code
COPY . .

# Run go mod tidy to ensure the go.mod file is up to date
RUN go mod tidy

# Build the application and capture the output
RUN go build -v -o /run-app . 

# Stage 2: Final stage
FROM debian:bookworm-slim

# Install CA certificates in the final image
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built executable from the builder stage
COPY --from=builder /run-app /usr/local/bin/run-app

# Create necessary directory
RUN mkdir -p /app/data

# Copy the CSV file to /app/data
COPY /data/ip_metadata.csv /app/data/ip_metadata.csv

# Set the working directory
WORKDIR /app

# Set the command to run the application
CMD ["/usr/local/bin/run-app"]