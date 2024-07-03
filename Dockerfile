ARG GO_VERSION=1.22.5
FROM golang:${GO_VERSION}-bookworm as builder

# Install CA certificates
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container to /usr/src/app
WORKDIR /usr/src/app

# Copy the Go module files first to leverage Docker cache for dependency layers
COPY go.mod go.sum ./

# Run module download separately to also leverage caching of downloaded modules
RUN go mod download && go mod verify

# Copy the CSV file into a data directory within the builder stage
COPY ip_metadata.csv ./data/ip_metadata.csv

# Copy the rest of the application source code
COPY . .

# Build the application; output the binary to a known location
RUN go build -v -o /run-app .

# Expose port 9000 if it's being used by the application
EXPOSE 9000

# Final stage based on Debian Bookworm.
FROM debian:bookworm

# Install CA certificates in the final image to ensure they are present.
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built executable from the builder stage
COPY --from=builder /run-app /usr/local/bin/run-app

# Copy the CSV and other data files from the builder stage to the runtime image
COPY --from=builder /usr/src/app/data /data

# Set the command to run the application
CMD ["/usr/local/bin/run-app"]