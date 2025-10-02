# ---------- build stage ----------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy csproj files first (restore cache)
COPY Shared/Shared.csproj Shared/
COPY ArticleService/ArticleService.csproj ArticleService/
RUN dotnet restore ArticleService/ArticleService.csproj

# Copy the rest of the repo
COPY . .
WORKDIR /src/ArticleService
RUN dotnet publish -c Release -o /app/publish

# ---------- runtime stage ----------
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "ArticleService.dll"]
