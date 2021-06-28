FROM golang:latest as builder

WORKDIR /app


COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o trading trading-server.go


CMD ["./trading"]