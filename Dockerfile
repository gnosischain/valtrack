FROM golang:1.22.5-bookworm as builder

# Install CA certificates and build essentials
RUN apt-get update && apt-get install -y ca-certificates build-essential && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the Go module files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the application; output the binary to a known location
RUN go build -v -o /run-app .

# Final stage based on Debian Bookworm-slim
FROM debian:bookworm-slim

# Install CA certificates in the final image to ensure they are present.
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built executable from the builder stage
COPY --from=builder /run-app /usr/local/bin/run-app

# Create a directory for the data
RUN mkdir -p /app/data

# Copy the CSV file
COPY data/ip_metadata.csv /app/data/ip_metadata.csv

# Set the command to run the application
CMD ["/usr/local/bin/run-app"]