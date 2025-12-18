

## Vendee Globe Data Engineering – Final Implementation

This repository represents the **finale of the Vendee Globe data engineering saga**. It builds on earlier implementations by introducing **Apache Airflow** for orchestration and applying **practical data engineering patterns** on top of established domain logic.

If you are new to the project, it is strongly recommended that you review the previous repositories first. They provide progressively simpler, more explanatory implementations of the same problem domain and clearly articulate the analytical goals.

---

### Project Lineage and Context

The Vendee Globe project has evolved through several iterations, each emphasizing different architectural and tooling decisions:

#### Spark + S3–oriented implementation

[https://github.com/danilojoncic/Vendee-Globe-Spark-S3](https://github.com/danilojoncic/Vendee-Globe-Spark-S3)
Focuses on distributed data processing using Apache Spark, an S3-compatible object store, and a Spark master with two workers.

#### Pure Python / Pandas implementation

[https://github.com/danilojoncic/Vendee-Globe-Data-Analysis](https://github.com/danilojoncic/Vendee-Globe-Data-Analysis)
Uses Python and pandas with Docker Compose for infrastructure and orchestration. While intentionally "far-fetched" from a production perspective, this repository contains the **most comprehensive explanation of the business logic, data model, and desired analytical outcomes**.

This repository should be viewed as the **production-leaning culmination** of those efforts, combining prior lessons with workflow orchestration and explicit data engineering concerns.

---

### High-Level Overview

<img width="1234" height="1115" alt="Setup Overview" src="https://github.com/user-attachments/assets/1e740b38-fc42-4e07-9883-391740ee87c4" />
<img width="922" height="521" alt="DAGs and Buckets Overview" src="https://github.com/user-attachments/assets/b1ad6f1e-afad-4657-aa3a-24515fc0220a" />

At a high level, the system ingests, processes, and aggregates Vendee Globe race data through a series of **Airflow-managed pipelines**. Data is staged and transformed across multiple object storage buckets, with a clear separation between **raw**, **processed**, and **curated** datasets.

Key concepts demonstrated in this implementation include:

* Bucket-based data lifecycle management
* Explicit pipeline boundaries and hand-offs
* Repeatable, idempotent batch processing
* Production-oriented orchestration patterns

---

### Apache Airflow Responsibilities

Apache Airflow is used to:

* Orchestrate ingestion and transformation workflows
* Enforce task dependencies and execution order
* Provide observability into pipeline state, retries, and failures

The architecture and DAG-to-bucket relationships are illustrated in the diagrams above, highlighting data flow and responsibility boundaries between pipeline stages.

---

### Requirements and Resource Considerations

Although fully containerized, this setup is **resource-intensive**. Running the full stack locally requires sufficient CPU and memory to support Airflow, object storage, and processing workloads simultaneously.

---

### Screenshots

The screenshots below provide a concrete view into the running system, including Airflow DAGs, task execution, logs, object storage layout, and resulting datasets.

<img width="635" height="305" alt="Airflow UI" src="https://github.com/user-attachments/assets/95d808e1-7f22-47b5-9dcd-12cf633d9124" />
<img width="640" height="715" alt="DAG View" src="https://github.com/user-attachments/assets/ca9671b8-1f14-4ed5-a173-dae5e24e4fd7" />
<img width="1470" height="840" alt="Pipeline Execution" src="https://github.com/user-attachments/assets/92fc1fbd-c91a-4720-a135-2f04d074d7ee" />
<img width="1470" height="838" alt="Task Details" src="https://github.com/user-attachments/assets/2f36c8ba-4ce4-4570-8c4c-130ff89c01a0" />
<img width="1470" height="839" alt="Logs" src="https://github.com/user-attachments/assets/a3264a1e-ab91-429b-9648-e4ac5fcd01d5" />
<img width="1469" height="839" alt="Bucket Layout" src="https://github.com/user-attachments/assets/b9b657d5-3dfa-41e9-ad9c-51df3dc993df" />
<img width="1165" height="687" alt="Processed Data" src="https://github.com/user-attachments/assets/b6560718-bc92-4380-b22a-781d34809c34" />
<img width="1172" height="689" alt="Curated Outputs" src="https://github.com/user-attachments/assets/7cccfe6c-44ae-4518-a62d-248a3b9714ec" />
<img width="1169" height="690" alt="Aggregations" src="https://github.com/user-attachments/assets/37f846a0-a81e-43cb-b5ac-61c71b24ebd9" />
<img width="1470" height="841" alt="Final Results" src="https://github.com/user-attachments/assets/4dad336f-a2bc-424c-b78c-4200065e7ddf" />
<img width="1171" height="687" alt="Object Storage" src="https://github.com/user-attachments/assets/7755800b-9bca-4484-b9b9-c6f026e97ffa" />
<img width="1171" height="690" alt="Bucket Contents" src="https://github.com/user-attachments/assets/7c0a0b03-d4bd-4068-9951-df2c0970a3a8" />
<img width="1470" height="840" alt="Monitoring" src="https://github.com/user-attachments/assets/ee09e9d9-e3fd-43ff-8c20-7321e0cd6db6" />
<img width="640" height="715" alt="Airflow Graph" src="https://github.com/user-attachments/assets/5ec23cfd-31ed-490e-be3a-0785d517cd81" />
<img width="645" height="405" alt="Task Success" src="https://github.com/user-attachments/assets/323cd364-3e6c-437e-9da3-f63a93235516" />

---

### Final Notes

This repository is intentionally focused on **orchestration, data flow clarity, and data engineering patterns**, rather than re-explaining the analytical problem itself. For a deeper dive into domain logic, metrics, and analytical intent, refer to the pandas-based implementation linked above.

Taken together, the three repositories form a coherent narrative: from analytical prototyping, to distributed processing, to fully orchestrated data engineering pipelines.



<img width="1171" height="690" alt="Screenshot 2025-12-18 at 10 51 11" src="https://github.com/user-attachments/assets/7c0a0b03-d4bd-4068-9951-df2c0970a3a8" />
<img width="1470" height="840" alt="Screenshot 2025-12-18 at 10 50 54" src="https://github.com/user-attachments/assets/ee09e9d9-e3fd-43ff-8c20-7321e0cd6db6" />

<img width="640" height="715" alt="Screenshot 2025-12-18 at 10 54 14" src="https://github.com/user-attachments/assets/5ec23cfd-31ed-490e-be3a-0785d517cd81" />
<img width="645" height="405" alt="Screenshot 2025-12-18 at 10 54 26" src="https://github.com/user-attachments/assets/323cd364-3e6c-437e-9da3-f63a93235516" />
