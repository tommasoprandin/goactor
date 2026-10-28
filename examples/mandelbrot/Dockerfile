FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /mandelbrot ./cmd/mandelbrot

FROM scratch
COPY --from=build /mandelbrot /mandelbrot
ENTRYPOINT ["/mandelbrot"]
