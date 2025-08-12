FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Convert.csproj ./
RUN dotnet restore Convert.csproj
COPY . .
RUN dotnet publish Convert.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
VOLUME ["/data/metadata", "/data/converted"]
ENTRYPOINT ["dotnet", "Convert.dll"]




