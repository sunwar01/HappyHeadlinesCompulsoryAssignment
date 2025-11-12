# ---------- build stage ----------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY PublisherService/PublisherService.csproj PublisherService/
RUN dotnet restore PublisherService/PublisherService.csproj

COPY Shared/ Shared/
COPY PublisherService/ PublisherService/
WORKDIR /src/PublisherService
RUN dotnet publish -c Release -o /app/publish /p:UseAppHost=false

# ---------- runtime stage ----------
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish ./
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "PublisherService.dll"]
