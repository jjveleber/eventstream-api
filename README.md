# EventStream API

**Production-ready real-time event aggregator** - polls GitHub, HackerNews, and Reddit APIs, normalizes their data into a unified event stream, and pushes updates to connected clients via Server-Sent Events (SSE).

Perfect for building real-time dashboards, monitoring feeds, and event-driven applications.

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![FastAPI 0.115](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)

---

## Features

- ⚡ **Real-time streaming** via Server-Sent Events (SSE)
- 🔄 **Background polling** from multiple sources (GitHub, HackerNews, Reddit)
- 📊 **Unified event schema** - normalized data from heterogeneous APIs
- 🗄️ **PostgreSQL storage** with event deduplication
- 🔐 **Optional API key authentication**
- 🚦 **Rate limiting** (60 req/min default, configurable)
- 🏥 **Health checks** for database and each event source
- 🐳 **Docker-ready** with docker-compose
- ✅ **>70% test coverage** (unit, integration, e2e with testcontainers)
- 📈 **Async all the way** - fully async Python with SQLAlchemy and httpx

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       External APIs                             │
│  GitHub Events │ HackerNews Top Stories │ Reddit Hot Posts     │
└────────┬────────────────┬───────────────────────┬──────────────┘
         │                │                       │
         ▼                ▼                       ▼
    ┌─────────────────────────────────────────────────┐
    │        Event Source Adapters (async)           │
    │   GitHubEventSource │ HNEventSource │ Reddit  │
    └────────────────┬────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  Background Polling   │
         │  (asyncio.gather)     │
         └──────────┬────────────┘
                    │
        ┌───────────┴──────────┐
        │                      │
        ▼                      ▼
  ┌──────────┐         ┌──────────────┐
  │PostgreSQL│         │SSE Connection│
  │ (dedupe) │         │   Manager    │
  └────┬─────┘         └──────┬───────┘
       │                      │
       ▼                      ▼
┌────────────┐        ┌─────────────────┐
│ REST API   │        │  SSE Streaming  │
│ /events    │        │  /stream        │
└────────────┘        └─────────────────┘
       │                      │
       └──────────┬───────────┘
                  ▼
          ┌──────────────┐
          │   Clients    │
          │ (Dashboard)  │
          └──────────────┘
```

**Key Components:**

1. **Event Source Adapters** (`app/sources/`) - Fetch and normalize events from external APIs
2. **Background Polling** (`app/main.py`) - Runs in lifespan context, polls sources at configured intervals
3. **SSE Connection Manager** (`app/streaming/sse.py`) - Manages WebSocket-like connections, broadcasts events
4. **REST API** (`app/api/`) - HTTP endpoints for event history and source status
5. **PostgreSQL** - Async SQLAlchemy with deduplication via `external_id`

---

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16+ (or use Docker)
- Docker & Docker Compose (optional)

### Option 1: Docker Compose (Recommended)

```bash
# Clone repository
git clone <repo-url>
cd eventstream-api

# Start services
docker-compose up

# API available at http://localhost:8000
# SSE stream: http://localhost:8000/api/stream
# API docs: http://localhost:8000/docs
```

### Option 2: Local Development

```bash
# 1. Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up PostgreSQL (or use Docker)
docker run -d \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=eventstream \
  -p 5432:5432 \
  postgres:16

# 4. Configure environment
cat > .env <<EOF
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/eventstream
LOG_LEVEL=INFO
GITHUB_POLL_INTERVAL=60
HACKERNEWS_POLL_INTERVAL=120
REDDIT_POLL_INTERVAL=120
EOF

# 5. Run server
uvicorn app.main:app --reload

# Server starts at http://localhost:8000
```

---

## API Usage

### REST Endpoints

#### Get Events (History)

```bash
# Get latest 100 events
curl http://localhost:8000/api/events

# Filter by source
curl "http://localhost:8000/api/events?source=github&limit=50"

# Pagination
curl "http://localhost:8000/api/events?offset=100&limit=50"
```

**Response:**
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "source": "github",
    "event_type": "PushEvent",
    "title": "user pushed 3 commits to repo/main",
    "url": "https://github.com/owner/repo",
    "external_id": "github_12345",
    "metadata": {
      "actor": "username",
      "repo": "owner/repo"
    },
    "timestamp": "2026-04-17T10:30:00Z",
    "created_at": "2026-04-17T10:30:05Z"
  }
]
```

#### Health Check

```bash
curl http://localhost:8000/api/health
```

**Response:**
```json
{
  "status": "healthy",
  "database": "healthy",
  "sources": [
    {
      "name": "github",
      "is_healthy": true,
      "last_poll": "2026-04-17T10:29:00Z",
      "last_success": "2026-04-17T10:29:00Z",
      "last_error": null
    }
  ]
}
```

#### Get Sources

```bash
curl http://localhost:8000/api/sources
```

**Response:**
```json
[
  {
    "name": "github",
    "base_url": "https://api.github.com",
    "poll_interval": 60
  },
  {
    "name": "hackernews",
    "base_url": "https://hacker-news.firebaseio.com/v0",
    "poll_interval": 120
  }
]
```

### Server-Sent Events (SSE)

#### Stream All Events

```bash
# Using curl
curl -N http://localhost:8000/api/stream

# Output (continuous stream):
event: new_event
data: {"id":"...","source":"github","event_type":"PushEvent",...}

event: new_event
data: {"id":"...","source":"hackernews","event_type":"story",...}
```

#### Filter by Source

```bash
# Only GitHub and Reddit events
curl -N "http://localhost:8000/api/stream?sources=github,reddit"
```

#### JavaScript Client

```html
<!DOCTYPE html>
<html>
<body>
  <div id="events"></div>
  <script>
    const evtSource = new EventSource('http://localhost:8000/api/stream?sources=github');
    
    evtSource.addEventListener('new_event', (event) => {
      const data = JSON.parse(event.data);
      console.log('New event:', data);
      
      // Display event
      document.getElementById('events').innerHTML += `
        <div>
          <strong>${data.source}</strong>: ${data.title}
          <a href="${data.url}">Link</a>
        </div>
      `;
    });
    
    evtSource.onerror = (err) => {
      console.error('SSE error:', err);
    };
  </script>
</body>
</html>
```

---

## Configuration

### Environment Variables

Create `.env` file in project root:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/eventstream

# Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR

# Polling Intervals (seconds)
GITHUB_POLL_INTERVAL=60
HACKERNEWS_POLL_INTERVAL=120
REDDIT_POLL_INTERVAL=120

# Event Retention
MAX_EVENTS_PER_SOURCE=1000  # Cleanup threshold per source

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_PER_MINUTE=60

# Authentication (optional)
API_KEY_ENABLED=false
API_KEY=your-secret-key-here
```

### Using API Key Authentication

```bash
# Enable in .env
API_KEY_ENABLED=true
API_KEY=my-secret-api-key

# Include in requests
curl -H "X-API-Key: my-secret-api-key" http://localhost:8000/api/events
```

---

## Testing

### Run All Tests

```bash
# Activate venv
source .venv/bin/activate

# Run with coverage
pytest --cov=app --cov-report=html --cov-report=term

# Coverage report saved to htmlcov/index.html
```

### Test Categories

- **Unit Tests** (`tests/unit/`) - Test individual components (adapters, SSE manager)
- **Integration Tests** (`tests/integration/`) - Test full polling flow with testcontainers (PostgreSQL)
- **E2E Tests** (`tests/e2e/`) - Test complete API workflows

### Example: Run Specific Test

```bash
# Test GitHub adapter
pytest tests/unit/test_sources.py::test_github_source -v

# Test SSE streaming
pytest tests/e2e/test_api.py::test_sse_stream -v
```

---

## Deployment

### Kubernetes (k8s)

Create `k8s/` directory with manifests:

#### 1. PostgreSQL StatefulSet

```yaml
# k8s/postgres.yaml
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  ports:
    - port: 5432
  selector:
    app: postgres
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:16
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_USER
          value: postgres
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: password
        - name: POSTGRES_DB
          value: eventstream
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
  - metadata:
      name: postgres-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

#### 2. EventStream API Deployment

```yaml
# k8s/api.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: eventstream-config
data:
  GITHUB_POLL_INTERVAL: "60"
  HACKERNEWS_POLL_INTERVAL: "120"
  REDDIT_POLL_INTERVAL: "120"
  LOG_LEVEL: "INFO"
  RATE_LIMIT_ENABLED: "true"
  RATE_LIMIT_PER_MINUTE: "60"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventstream-api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: eventstream-api
  template:
    metadata:
      labels:
        app: eventstream-api
    spec:
      containers:
      - name: api
        image: <your-registry>/eventstream-api:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: database-url
        envFrom:
        - configMapRef:
            name: eventstream-config
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: eventstream-api
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8000
  selector:
    app: eventstream-api
```

#### 3. Create Secrets

```bash
# Create secret for PostgreSQL
kubectl create secret generic postgres-secret \
  --from-literal=password='your-secure-password' \
  --from-literal=database-url='postgresql+asyncpg://postgres:your-secure-password@postgres:5432/eventstream'
```

#### 4. Apply Manifests

```bash
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/api.yaml

# Check status
kubectl get pods
kubectl logs -f deployment/eventstream-api
```

---

### AWS Deployment

#### ECS with Fargate

1. **Build and push Docker image:**

```bash
# Build image
docker build -t eventstream-api .

# Tag for ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
docker tag eventstream-api:latest <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/eventstream-api:latest
docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/eventstream-api:latest
```

2. **Set up RDS PostgreSQL:**

```bash
aws rds create-db-instance \
  --db-instance-identifier eventstream-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --engine-version 16 \
  --master-username postgres \
  --master-user-password <password> \
  --allocated-storage 20
```

3. **Create ECS task definition:**

```json
{
  "family": "eventstream-api",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "containerDefinitions": [
    {
      "name": "eventstream-api",
      "image": "<aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/eventstream-api:latest",
      "portMappings": [{"containerPort": 8000, "protocol": "tcp"}],
      "environment": [
        {"name": "GITHUB_POLL_INTERVAL", "value": "60"},
        {"name": "LOG_LEVEL", "value": "INFO"}
      ],
      "secrets": [
        {
          "name": "DATABASE_URL",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:<account>:secret:eventstream-db-url"
        }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "python -c \"import httpx; httpx.get('http://localhost:8000/api/health')\""],
        "interval": 30,
        "timeout": 5,
        "retries": 3
      }
    }
  ]
}
```

4. **Create ECS service with ALB:**

```bash
aws ecs create-service \
  --cluster eventstream-cluster \
  --service-name eventstream-api \
  --task-definition eventstream-api \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:us-east-1:xxx:targetgroup/eventstream-tg,containerName=eventstream-api,containerPort=8000"
```

---

### GCP Deployment (Cloud Run)

```bash
# 1. Build and push to Artifact Registry
gcloud builds submit --tag gcr.io/<project-id>/eventstream-api

# 2. Create Cloud SQL PostgreSQL instance
gcloud sql instances create eventstream-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1

# 3. Create database
gcloud sql databases create eventstream --instance=eventstream-db

# 4. Deploy to Cloud Run
gcloud run deploy eventstream-api \
  --image gcr.io/<project-id>/eventstream-api \
  --platform managed \
  --region us-central1 \
  --add-cloudsql-instances <project-id>:us-central1:eventstream-db \
  --set-env-vars DATABASE_URL=postgresql+asyncpg://postgres:<password>@/eventstream?host=/cloudsql/<project-id>:us-central1:eventstream-db \
  --set-env-vars GITHUB_POLL_INTERVAL=60,LOG_LEVEL=INFO \
  --allow-unauthenticated \
  --max-instances 5
```

---

### Azure Deployment (Container Instances)

```bash
# 1. Create resource group
az group create --name eventstream-rg --location eastus

# 2. Create Azure Database for PostgreSQL
az postgres flexible-server create \
  --resource-group eventstream-rg \
  --name eventstream-db \
  --location eastus \
  --admin-user postgres \
  --admin-password <password> \
  --sku-name Standard_B1ms \
  --version 16

# 3. Create database
az postgres flexible-server db create \
  --resource-group eventstream-rg \
  --server-name eventstream-db \
  --database-name eventstream

# 4. Build and push to Azure Container Registry
az acr create --resource-group eventstream-rg --name eventstream --sku Basic
az acr build --registry eventstream --image eventstream-api:latest .

# 5. Deploy to Container Instances
az container create \
  --resource-group eventstream-rg \
  --name eventstream-api \
  --image eventstream.azurecr.io/eventstream-api:latest \
  --cpu 1 \
  --memory 1 \
  --ports 8000 \
  --environment-variables \
    DATABASE_URL=postgresql+asyncpg://postgres:<password>@eventstream-db.postgres.database.azure.com/eventstream \
    GITHUB_POLL_INTERVAL=60 \
    LOG_LEVEL=INFO \
  --dns-name-label eventstream-api
```

---

## Monitoring & Observability

### Logs

```bash
# Docker Compose
docker-compose logs -f api

# Kubernetes
kubectl logs -f deployment/eventstream-api

# AWS ECS
aws logs tail /ecs/eventstream-api --follow
```

### Metrics Endpoint

```bash
curl http://localhost:8000/api/metrics
```

**Response:**
```json
{
  "sse_connections": 5,
  "sources": {
    "github": {
      "is_healthy": true,
      "last_poll": "2026-04-17T10:29:00Z",
      "last_success": "2026-04-17T10:29:00Z",
      "error_count": 0,
      "total_events": 1543
    }
  }
}
```

---

## Troubleshooting

### Issue: Database connection fails

```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Test connection
psql postgresql://postgres:postgres@localhost:5432/eventstream

# Check DATABASE_URL format
# Correct: postgresql+asyncpg://user:pass@host:port/db
# Wrong: postgresql://... (missing +asyncpg)
```

### Issue: No events appearing in stream

```bash
# 1. Check source health
curl http://localhost:8000/api/health

# 2. Check logs for polling errors
docker-compose logs api | grep "Error polling"

# 3. Test external API manually
curl https://api.github.com/events
```

### Issue: SSE connection closes immediately

```bash
# Check rate limiting
curl -I http://localhost:8000/api/stream
# Look for: X-RateLimit-Remaining

# Increase limit in .env
RATE_LIMIT_PER_MINUTE=120
```

### Issue: High memory usage

```bash
# Reduce event retention
MAX_EVENTS_PER_SOURCE=500

# Limit active SSE connections
# Add to middleware.py:
MAX_SSE_CONNECTIONS = 100
```

---

## Adding New Event Sources

1. **Create adapter** in `app/sources/newsource.py`:

```python
from app.sources.base import BaseEventSource
from app.schemas import EventCreate

class NewSource(BaseEventSource):
    source_name = "newsource"
    base_url = "https://api.example.com"
    poll_interval = 180

    async def fetch_events(self, limit: int = 100) -> list[EventCreate]:
        data = await self._make_request("/endpoint")
        # Normalize to EventCreate schema
        return [EventCreate(...) for item in data]
```

2. **Register** in `app/sources/registry.py`:

```python
from app.sources.newsource import NewSource

ACTIVE_SOURCES = [
    GitHubEventSource(),
    HackerNewsEventSource(),
    RedditEventSource(),
    NewSource(),  # Add here
]
```

3. **Add config** in `app/config.py`:

```python
class Settings(BaseSettings):
    newsource_poll_interval: int = 180
```

4. **Test** in `tests/unit/test_sources.py`:

```python
@pytest.mark.asyncio
@respx.mock
async def test_newsource():
    respx.get("https://api.example.com/endpoint").mock(
        return_value=httpx.Response(200, json=[...])
    )
    source = NewSource()
    events = await source.fetch_events()
    assert len(events) > 0
```

---

## Performance Tips

- **Database indexing**: Already optimized (`idx_source_timestamp`)
- **Connection pooling**: Set in `app/database.py` (pool_size=10)
- **Horizontal scaling**: Run multiple API instances behind load balancer (polling task runs in each, dedupe via `external_id`)
- **Caching**: Add Redis for frequently-accessed events
- **CDN**: Serve static assets via CloudFlare/CloudFront

---

## License

MIT License - see LICENSE file

---

## Contributing

1. Fork repository
2. Create feature branch (`git checkout -b feature/new-source`)
3. Commit changes (`git commit -m 'Add new source adapter'`)
4. Push to branch (`git push origin feature/new-source`)
5. Open Pull Request

---

## Support

- **Issues**: [GitHub Issues](https://github.com/<your-repo>/issues)
- **Docs**: `/docs` endpoint (Swagger UI)
- **Email**: support@example.com

---

**Built with ❤️ for real-time event streaming**
