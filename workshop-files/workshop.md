# Redis in Cloud-Native Environments Workshop

## 🎯 Workshop Objectives

By the end of this workshop, you will understand:
- How Redis works as an external state store in microservices
- Redis data structures and their use cases in time-series applications
- Configuration separation in cloud-native applications
- Deploying Redis-backed applications to Kubernetes
- Monitoring and debugging Redis in containerized environments

## 📋 Prerequisites

- Docker Desktop installed and running
- Basic understanding of containers and microservices
- Terminal/command line familiarity
- (Optional) Kubernetes knowledge for Part 2

---

## Part 1: Redis with Docker Compose 

### Step 1: Setup and Initial Exploration 

1. **Clone and navigate to the project:**
   ```bash
   cd /path/to/docker-redis-time-series/prediction-api
   ```

2. **Explore the project structure:**
   ```bash
   ls -la
   cat docker-compose.yml
   ```

3. **Start the services:**
   ```bash
   docker-compose up -d
   ```

4. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

### Step 2: Understanding Redis as External State Store (15 minutes)

#### Exercise 2.1: Redis Data Exploration

1. **Connect to Redis container:**
   ```bash
   docker-compose exec redis redis-cli
   ```

2. **Explore Redis data structures:**
   ```bash
   # List all keys
   KEYS *
   
   # Check Redis info
   INFO memory
   INFO keyspace
   ```

3. **Exit Redis CLI:**
   ```bash
   exit
   ```

#### Exercise 2.2: Test the API and Observe Redis Usage

1. **Test the health endpoint:**
   ```bash
   curl http://localhost:5001/health
   ```

2. **Make a prediction with historical data:**
   ```bash
   curl -X POST http://localhost:5001/predict \
     -H "Content-Type: application/json" \
     -d '{
       "series_id": "workshop-demo",
       "historical_data": [
         {"timestamp": "2025-01-01T00:00:00", "value": 100.0},
         {"timestamp": "2025-01-01T01:00:00", "value": 101.0},
         {"timestamp": "2025-01-01T02:00:00", "value": 102.0}
       ]
     }'
   ```

3. **Check what was stored in Redis:**
   ```bash
   docker-compose exec redis redis-cli
   KEYS *
   ```

4. **Examine the data structures:**
   ```bash
   # Check the timeline (sorted set)
   ZRANGE timeline:workshop-demo 0 -1
   
   # Check individual data points
   GET timeseries:workshop-demo:2025-01-01T00:00:00
   
   # Check active series
   SMEMBERS series:active
   
   # Check recent cache
   GET recent:workshop-demo
   ```

5. **Exit Redis CLI:**
   ```bash
   exit
   ```

#### Exercise 2.3: Add More Data and Observe Patterns

1. **Add more data points:**
   ```bash
   curl -X POST http://localhost:5001/add_data \
     -H "Content-Type: application/json" \
     -d '{
       "series_id": "workshop-demo",
       "data_points": [
         {"timestamp": "2025-01-01T03:00:00", "value": 103.0},
         {"timestamp": "2025-01-01T04:00:00", "value": 104.0}
       ]
     }'
   ```

2. **Make another prediction:**
   ```bash
   curl -X POST http://localhost:5001/predict \
     -H "Content-Type: application/json" \
     -d '{"series_id": "workshop-demo"}'
   ```

3. **Check Redis data growth:**
   ```bash
   docker-compose exec redis redis-cli
   KEYS *
   ZCARD timeline:workshop-demo
   ```

### Step 3: Understanding Configuration Separation (10 minutes)

#### Exercise 3.1: Explore Configuration Files

1. **Examine the configuration structure:**
   ```bash
   cat config.py
   cat env.development
   cat env.production
   ```

2. **Check environment variables in containers:**
   ```bash
   docker-compose exec api env | grep -E "(REDIS|MODEL|FLASK)"
   ```

3. **Modify configuration and restart:**
   ```bash
   # Edit docker-compose.yml to change LOOKBACK_WINDOW to 10
   # Then restart
   docker-compose restart api
   ```

### Step 4: Redis Monitoring and Debugging 

#### Exercise 4.1: Monitor Redis Performance

1. **Check Redis logs:**
   ```bash
   docker-compose logs redis
   ```

2. **Monitor Redis in real-time:**
   ```bash
   docker-compose exec redis redis-cli MONITOR
   # In another terminal, make API calls to see Redis commands
   ```

3. **Check Redis memory usage:**
   ```bash
   docker-compose exec redis redis-cli INFO memory
   ```

#### Exercise 4.2: Test Redis Persistence
 e
1. **Check Redis persistence settings:**
   ```bash
   docker-compose exec redis redis-cli CONFIG GET save
   ```

2. **Simulate container restart:**
   ```bash
   docker-compose restart redis
   ```

3. **Verify data persistence:**
   ```bash
   docker-compose exec redis redis-cli KEYS *
   ```

---

## Part 2: Kubernetes Deployment 

### Step 5: Kubernetes Setup and Deployment 

#### Exercise 5.1: Deploy to Kubernetes

1. **Verify Kubernetes is running:**
   ```bash
   kubectl cluster-info
   ```

2. **Deploy using the provided script:**
   ```bash
   # Navigate to the k8s directory in the main project
   cd ../prediction-api/k8s
   ./deploy-docker-desktop.sh
   ```

3. **Monitor the deployment:**
   ```bash
   kubectl get pods -n prediction-api -w
   ```

#### Exercise 5.2: Verify Kubernetes Deployment

1. **Check all resources:**
   ```bash
   kubectl get all -n prediction-api
   ```

2. **Test the API:**
   ```bash
   curl http://localhost:30080/health
   curl http://localhost:30080/
   ```

### Step 6: Redis in Kubernetes Environment  

#### Exercise 6.1: Compare Redis Behavior

1. **Test the same prediction in Kubernetes:**
   ```bash
   curl -X POST http://localhost:30080/predict \
     -H "Content-Type: application/json" \
     -d '{
       "series_id": "k8s-workshop",
       "historical_data": [
         {"timestamp": "2025-01-01T00:00:00", "value": 50.0},
         {"timestamp": "2025-01-01T01:00:00", "value": 52.0},
         {"timestamp": "2025-01-01T02:00:00", "value": 54.0}
       ]
     }'
   ```

2. **Access Redis in Kubernetes:**
   ```bash
   kubectl exec -it deployment/redis -n prediction-api -- redis-cli
   KEYS *
   ZRANGE timeline:k8s-workshop 0 -1
   exit
   ```

#### Exercise 6.2: Configuration Management in Kubernetes

1. **Check ConfigMap:**
   ```bash
   kubectl get configmap prediction-api-config -n prediction-api -o yaml
   ```

2. **Check Secrets:**
   ```bash
   kubectl get secret prediction-api-secrets -n prediction-api -o yaml
   ```

3. **Check environment variables in pods:**
   ```bash
   kubectl exec deployment/prediction-api -n prediction-api -- env | grep -E "(REDIS|MODEL|FLASK)"
   ```

#### Exercise 6.3: Scaling and High Availability

1. **Scale the API deployment:**
   ```bash
   kubectl scale deployment prediction-api --replicas=3 -n prediction-api
   ```

2. **Verify scaling:**
   ```bash
   kubectl get pods -n prediction-api
   ```

3. **Test load distribution:**
   ```bash
   # Make multiple requests and check logs
   for i in {1..5}; do
     curl -X POST http://localhost:30080/predict \
       -H "Content-Type: application/json" \
       -d "{\"series_id\": \"load-test-$i\", \"historical_data\": [{\"timestamp\": \"2025-01-01T00:00:00\", \"value\": $((100+i))}]}"
   done
   ```

4. **Check which pods handled the requests:**
   ```bash
   kubectl logs deployment/prediction-api -n prediction-api
   ```

---

## 🧪 Hands-On Exercises

### Exercise A: Redis Data Structure Analysis
**Goal**: Understand how different Redis data structures are used

1. Make several API calls with different series IDs
2. Connect to Redis and analyze the data structures:
   - `timeseries:*` (strings) - Individual data points
   - `timeline:*` (sorted sets) - Time-ordered data
   - `series:active` (set) - Active series tracking
   - `recent:*` (strings) - Cached recent data
   - `predictions:*` (strings) - Prediction history

### Exercise B: Configuration Testing
**Goal**: Understand configuration separation

1. Modify the ConfigMap in Kubernetes:
   ```bash
   kubectl edit configmap prediction-api-config -n prediction-api
   ```
2. Change `LOOKBACK_WINDOW` from 20 to 5
3. Restart the deployment and test predictions
4. Observe how the prediction behavior changes

### Exercise C: Redis Persistence Testing
**Goal**: Verify Redis data persistence

1. Add data via the API
2. Delete the Redis pod: `kubectl delete pod -l app=redis -n prediction-api`
3. Wait for the new pod to start
4. Verify data is still available

---

## 🔍 Key Learning Points

### Redis as External State Store
- **Why Redis?**: Fast, in-memory, supports complex data structures
- **Data Structures**: Strings, Sets, Sorted Sets for different use cases
- **Persistence**: AOF and RDB for data durability
- **Scalability**: Single-instance vs. cluster considerations

### Configuration Management
- **Environment Variables**: Runtime configuration injection
- **ConfigMaps**: Non-sensitive configuration in Kubernetes
- **Secrets**: Sensitive data management
- **Configuration Classes**: Environment-specific settings

### Cloud-Native Patterns
- **Service Discovery**: How services find each other
- **Health Checks**: Liveness and readiness probes
- **Scaling**: Horizontal pod autoscaling
- **Persistence**: Persistent volumes for stateful data

---

## 🧹 Cleanup

### Docker Compose Cleanup
```bash
docker-compose down -v
```

### Kubernetes Cleanup
```bash
kubectl delete namespace prediction-api
```

---

## 📚 Additional Resources

- [Redis Documentation](https://redis.io/documentation)
- [Kubernetes ConfigMaps and Secrets](https://kubernetes.io/docs/concepts/configuration/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Redis Data Types](https://redis.io/docs/data-types/)

---

## 🎯 Workshop Outcomes

After completing this workshop, you should be able to:
- ✅ Explain how Redis serves as an external state store
- ✅ Identify appropriate Redis data structures for different use cases
- ✅ Deploy containerized applications with Redis to Kubernetes
- ✅ Manage configuration in cloud-native environments
- ✅ Monitor and debug Redis in containerized environments
- ✅ Understand the benefits of configuration separation
