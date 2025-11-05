# ---------- build stage ----------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy only NewsletterService + Shared
COPY Shared/Shared.csproj Shared/
COPY NewsletterService/NewsletterService.csproj NewsletterService/

# Restore
RUN dotnet restore NewsletterService/NewsletterService.csproj

# Copy source
COPY Shared/ Shared/
COPY NewsletterService/ NewsletterService/

# Copy ONLY the model from SubscriberService (we don't need its API)
COPY SubscriberService/Models/Subscriber.cs SubscriberService/Models/

# Build & publish
WORKDIR /src/NewsletterService
RUN dotnet publish -c Release -o /app/publish /p:UseAppHost=false

# ---------- runtime stage ----------
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "NewsletterService.dll"]