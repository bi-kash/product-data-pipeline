# Project Brief: Modular Product Data Pipeline

**Vision**
The overarching goal of this project is to build an automated and scalable end-to-end
pipeline. This pipeline will identify jewelry products from the AliExpress platform,
qualify them based on a precise set of rules, and ultimately prepare them for final
processing and listing by our virtual assistants (VAs) in a central and user-friendly work
environment (Airtable).

**Technical Guardrails (Principles for the Entire Project)**

- **Architecture:** The entire codebase must be designed to be modular from the
    ground up. This means that logically related functions will be structured in
    separate directories (e.g., /harvester, /processor, /common). Every main
    process step, such as collecting merchants or processing products, must be
    executable via its own clearly named command in the command-line interface
    (CLI). Additionally, all data-intensive commands must support the safety
    switches --limit X (processes only the first X entries) and --dry-run (simulates the
    run without writing data).
- **Runtime Environment:** The target runtime environment for all scripts is a Linux
    system with Python 3.10 or newer. The scripts must be designed to run
    "headless," i.e., without a graphical user interface, to allow for future
    automation via a cron job. The README.md file must include clear instructions
    for setting up the development environment using python -m venv and pip install -r requirements.txt.
- **Database:** As the central data system, we will use a professional cloud
    database, preferably PostgreSQL (e.g., via a provider like Supabase). It is a
    crucial requirement that the database stores both the complete, unaltered raw
    data from the API (for example, in a raw_json column) and the clean,
    "normalized" data that we process and prepare. All timestamps in the database
    (e.g., in job_runs, first_seen_at) must be stored consistently in UTC to ensure
    server-independent consistency.
- **No Binary Data in DB:** To keep the database lean and performant, it is a firm
    requirement that **no image files (binary data) are stored directly in the**
    **PostgreSQL database.** The database will exclusively store metadata about the
    images (such as URLs, hashes, or paths to embedding files). The actual image
    files will be stored in a dedicated cloud storage solution (S3).


- **Keys:** To uniquely identify products across all system boundaries, we will
    consistently use the combination of seller_id and item_id as a composite
    primary key.
- **Configuration:** No critical settings such as API keys, price limits, or keyword
    lists may be hardcoded in the source code. The search terms for the merchant
    harvest must be located in an external file like keywords.csv. All other
    parameters must be managed in an external configuration file (preferably in .env
    format). As part of the handover, a sample keywords.csv and an .env.example
    file with all necessary variables are expected.
- **Region & Currency:** To obtain correct prices and delivery times, all requests to
    the AliExpress API or any scraper sessions must be configured to set the
    **shipping destination to Germany (DE)**. All prices must be consistently
    converted to the **EUR** currency for comparisons and calculations.
- **Code Ownership & Version Control:** All work will be conducted in a private
    GitHub repository provided by me, with traceable commits. All source code and
    created artifacts will become my sole property upon full payment for the
    respective milestone.

# The Project Milestones

**_Milestone 1: Module A – Merchant Harvest & Verification_**

- **Objective:** This first module creates the foundation for the entire process by
    building and maintaining a comprehensive, yet verified, database of all relevant
    jewelry merchants. It collects merchants, supports a workflow for manual
    review by VAs, and ensures a clean data foundation.
- **Tasks & Deliverables:**
    - **Quick-Check & Ingest Client:** At the beginning, you will conduct a
       thorough test of the AliExpress API. Subsequently, you will develop a
       robust Python script that collects merchant IDs via a broad product
       search, intelligently handling paging, rate limits, and timeouts.
    - **DB Schema & Logging:** You will design and create the tables sellers (with
       fields like seller_id, shop_url, approval_status, first_seen_at,
       last_seen_at) and a detailed job_runs table to log every script execution
       with start/end times, duration, counts (found/new/skipped/errors), and
       job type.
    - **Logic & Delta Runs:** The core logic must be idempotent ("upsert"
       command) to avoid duplicates. New merchants will automatically receive
       the approval_status "PENDING". The possible status values are fixed to PENDING, WHITELIST, BLACKLIST. Module B will later process only merchants with WHITELIST status. The system must support daily "Delta Runs".

- **VA Interface for Merchant Verification:** To enable a controlled and
    verifiable review process, you will create two CLI commands:
       1. review:export-pending: This command exports all
          merchants with the status "PENDING" into a CSV file. To avoid
          compatibility issues, the CSV format will be clearly defined as
          **UTF-8 encoded, with a comma as the delimiter** , and will contain
          the columns seller_id, shop_url, approval_status, and note.
       2. review:import-results: This command reads a CSV file
          edited by the VA (with the same format) and updates the
          approval_status in the database accordingly.
- **Handover:** The handover includes the clean code in the Git repo, a
    README.md file with detailed instructions for setup and execution of the
    explicitly named CLI commands (harvest:init, harvest:delta,
    harvest:status, review:export-pending, review:import-results), and a
    "dashboard-light" in the form of a small text file with 2-3 simple SQL
    queries.
- **Acceptance Criteria:** The script must be idempotent and stable, timestamps
must be maintained correctly, and the harvest:status command must display
correct counts from the job_runs table. The CSV export/import workflow for
merchant verification must function reliably.

**_Milestone 2: Module B – Product Ingest, Filtering & Categorization_**

- **Objective:** This module processes **exclusively merchants with the**
    **approval_status "WHITELIST"**. It fetches their products and applies a set of
    predefined business rules.
- **Tasks & Deliverables:**
    - **Price Rule (Specified):** A business rule is applied at the **variant level**.
       Only products for which the price of the **most expensive variant** plus the
       cost of the cheapest shipping option to Germany does not exceed €
       will be processed further.
    - **Shipping & ETA Rule:** Only products with a fast delivery time (eta_days
       below a configurable limit) will be considered. The script will check for
       "Choice"/"Local+" badges and store the result ("Choice", "Local+",
       "Standard") in a shipping_type field. This check will be done via the API
       first; if ETA/badges are not available there, targeted **enrichment scraping**
       for these fields is part of the task.


- **Product Classification:** Products will be automatically classified into
    "Fine Jewelry" or "Fashion Jewelry" based on keywords.
- **Image Ingestion:** In this module, all available image URLs (hero, gallery,
    and variant) for each product will be collected and stored in a dedicated
    images table in the database. This table should include the columns
    image_role (with values like 'hero', 'gallery', 'variant'), variant_key (e.g.,
    the color), sort_index (for ordering), and optionally width/height to ensure
    clean mapping for subsequent modules.
- **Output:** The qualified products and their associated image information
    will be stored in the corresponding database tables.

**_Milestone 3: Module C – Duplicate Detection & Selection_**

- **Objective:** An intelligent pipeline to identify visual duplicates and select the
    most commercially viable offer.
- **Tasks & Deliverables:**
    - **Image Strategy for AI Analysis:** The number of images (hero and variant)
       to be processed for duplicate detection is controllable via the .env file.
    - **CLIP Execution:** The implementation of the CLIP analysis should, by
       default, not require a dedicated GPU (CPU-capable). If a GPU or a paid
       API is necessary for a performant implementation, this must be reported
       beforehand and approved separately.
    - **Two-Stage Detection:** First, a fast pHash comparison is performed. Only
       the remaining candidates are subjected to a semantic CLIP analysis. The
       embeddings will be stored locally.
    - **Master Selection:** For detected duplicate groups, the system will
       automatically select the product with the **lowest total_landed_cost**
       **(total_landed_cost = item_price_eur + shipping_cost_eur)** as the
       "master" product. In case of a price tie, a deterministic "tie-breaker" will
       ensure consistent results.
    - **Output:** Each product in the database will be marked as UNIQUE,
       MASTER, or DUPLICATE_OF_.... Ambiguous cases will be marked as
       REVIEW_SUSPECT.
- **Acceptance Criteria:** The thresholds for pHash and CLIP are configurable via
    the .env file and documented in the README.md.


**_Milestone 4: Module D – Airtable Synchronization & VA Workflow_**

- **Objective:** To prepare and deliver the qualified data to Airtable in a way that
    allows VAs to work efficiently and without knowledge of the original product
    source.
- **Tasks & Deliverables:**
    1. **Image Pipeline:** The script will download all relevant images from the
       URLs collected in Module B and upload them to an **Amazon S3 bucket**
       provided by us. Filenames will be anonymized (e.g., using UUIDs).
       Additionally, a simple lifecycle rule for the S3 bucket should be proposed
       to optimize costs.
    2. **Data Batching to Airtable:** The synchronization to Airtable must be
       controlled and robust. The explicitly named sync command airtable:sync
       must respect API limits (with retry/backoff) and support the --limit, --filter,
       and --dry-run switches.
    **3. Airtable Setup (Two-Table System):**
       - As part of the handover, either a small setup script or a detailed
          list in the README is expected to create the Airtable tables and
          fields exactly as specified.
       - **Table 1: "VA Workspace":** Contains an anonymous product_id,
          title, descriptions, the anonymous S3 image links, and two
          separate price fields: item_price_eur and shipping_cost_eur. A
          third formula field, selling_price_eur, will calculate the final price.
          A field named shipping_priority will show a neutral priority (e.g.,
          "Priority A", "Standard"). It is explicit that **no AliExpress-specific**
          **terms or IDs** will be displayed in this VA-visible table.
       - **Table 2: "Source Mapping" (for admins only):** Links the
          anonymous product_id to the original aliexpress_url.
    4. **VA Workflow:** The VAs will work exclusively in the "VA Workspace" table.
       They will review the images via the S3 links, can edit them externally if
       needed, and will then set the status in Airtable to "Images checked &
       ready".

**Collaboration & Scope Management**

- **Scope Definition:** The scope of this contract (Version 1.0) is strictly limited to
    the tasks and deliverables described in the four modules. Any additional ideas,
    requests, or new requirements that arise during the project will be collected in a separate backlog and can be commissioned as standalone follow-up projects after this contract is completed.

- **Communication Protocol:** You are expected to proactively clarify any
    uncertainties about the requirements before implementation. Any changes or
    extensions beyond the scope defined here require my prior, explicit, written
    approval to be valid.

**What Your Proposal Should Include:**
Please provide the following information for each module (A, B, C, and D):

- Your fixed price.
- A rough estimate of the time required or a timeline.
- A brief description of your technical approach (e.g., how you will handle rate
    limits).
- The most important libraries or tools you would use for the implementation.


