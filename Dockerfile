# Исходный образ с Golang
FROM golang:1.23

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы проекта в контейнер
COPY . .

# Устанавливаем зависимости и собираем проект
RUN go mod tidy
RUN go build -o chat-service cmd/main.go

# Запускаем микросервис
CMD ["./chat-service"]
