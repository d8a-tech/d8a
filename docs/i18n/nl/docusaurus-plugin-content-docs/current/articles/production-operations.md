# Productie-operaties

## Deploymentopties

Op dit moment documenteert d8a twee praktische deployment-vormen:

- **Single-node Docker Compose**: de eenvoudigste manier om d8a met ClickHouse op één machine te draaien. Zie de [Aan de slag-gids](/nl/getting-started).
- **Gesplitste processen op je eigen infrastructuur**: draai `d8a receiver` en `d8a worker` afzonderlijk, wat de aanbevolen basis is voor meer productiegerichte opstellingen.

Als je andere deploymentopties bouwt en onderhoudt, zoals Helm charts, Kubernetes-manifests of Terraform-gebaseerde voorbeelden, dan zijn bijdragen zeer welkom.

## HA

D8A ondersteunt het afzonderlijk draaien van de HTTP-receiver en de background worker, wat HA-opstellingen mogelijk maakt.

Beperkingen:

- Meerdere receivers worden ondersteund.
- Slechts één enkele worker wordt ondersteund (de session-status leeft in de lokale BoltDB van de worker). Horizontaal schalen van workers is niet mogelijk, dus je bent aangewezen op verticaal schalen, maar dat zou geen groot probleem moeten zijn—een machine met 16 cores en 30 GB RAM zou verkeer van rond de 7K reqps aankunnen, wat neerkomt op ongeveer 7B events/maand. Voor ~100M verkeer zou je met 2 CPU's en 4 GB RAM goed moeten zitten.

### Modi

- `d8a server`: receiver + worker in één proces (standaard).
- `d8a receiver`: alleen receiver (HTTP-server; publiceert naar de queue).
- `d8a worker`: alleen worker (consumeert uit de queue; geen HTTP-server).

### Queue-backends

Er bestaan twee queue-backends voor de grens tussen receiver/worker:

- `filesystem` (standaard): een lokale directory-queue.
- `objectstorage`: een gedeelde, object-storage-backed queue (Go CDK `blob.Bucket`).

Gebruik `objectstorage` wanneer je meerdere receivers op verschillende nodes draait.

### Voorbeeld: MinIO (S3-compatibel)

YAML (`config.yaml`):

```yaml
queue:
  backend: object_storage
  
  object_storage:
    prefix: d8a/dev/queue
    type: s3
    s3:
      host: 127.0.0.1
      port: 9000
      protocol: http
      bucket: d8a-queue
      access_key: minioadmin
      secret_key: minioadmin
      region: us-east-1
      create_bucket: true
```

Draai de receiver(s):

```bash
go run . receiver --config config.yaml --server-port 8080
```

Draai de worker (één instantie):

```bash
go run . worker --config config.yaml --storage-bolt-directory ./state
```

Opmerkingen:

- Gebruik `queue.object_storage.prefix` om omgevingen te namespacen (voorkomt overspraak binnen een gedeelde bucket).
- Het systeem werkt at-least-once: taken kunnen opnieuw worden afgespeeld als de worker crasht na verwerking maar vóór verwijdering.
