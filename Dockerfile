FROM docker.io/golang:1.22.4
LABEL authors="Gustavo Luvizotto Cesar"

WORKDIR /

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o /cloud-ip-prefixes

ENTRYPOINT ["/cloud-ip-prefixes"]
