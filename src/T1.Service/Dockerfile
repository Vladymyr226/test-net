# Build context: repository root (see docker-compose.yml)
ARG DOTNET_SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0
ARG DOTNET_RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0

FROM ${DOTNET_SDK_IMAGE} AS build
WORKDIR /src

COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/T1.Core/T1.Core.csproj src/T1.Core/
COPY src/T1.Service/T1.Service.csproj src/T1.Service/
RUN dotnet restore src/T1.Service/T1.Service.csproj

COPY db/ db/
COPY src/ src/
RUN dotnet publish src/T1.Service/T1.Service.csproj -c Release -o /app --no-restore

FROM ${DOTNET_RUNTIME_IMAGE} AS runtime
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
EXPOSE 8080
ENTRYPOINT ["dotnet", "T1.Service.dll"]
