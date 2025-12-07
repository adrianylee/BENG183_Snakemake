# Snakemake for RNA-seq  
###### BENG 183 – Sustainable Data Analysis with Workflow Managers  

---

1. [Abstract](#1)
2. [Introduction: Why Workflow Automation? Why Snakemake?](#2)  
3. [RNA-seq & Typical Pipelines](#3)  
4. [Snakemake Basics: Rules, DAGs, and Wildcards](#4)  
   3.0. [Getting Started](#40)  
   3.1. [Rules & `rule all`](#41)  
   3.2. [DAG Construction & Incremental Runs](#42)  
   3.3. [Wildcards](#43)  
   3.4. [Configs, Resources, and Execution Backends](#44)  
5. [Worked Example: RNA-seq Pipeline in Snakemake](#5)  
   5.0. [Simplified Snakefile](#50) 
6. [Limitations, Extensions, and Switching to Snakemake](#6)  
7. [Further Applications](#7)
8. [References](#8)  

---

## 1. Introduction: Why Workflow Automation? Why Snakemake?<a name="1"></a>
Workflow automation uses software to automate repetitive tasks and processes. Rather than sequentially running individual commands and manually tracking file dependencies, parameters, and intermediate outputs, workflow automation ensures that each task is executed in the correct order and with the appropriate inputs. **Snakemake** has become one of the two most widely adopted bioinformatics workflow management systems in computational biology because of its clear syntax, strong emphasis on reproducibility, and ability to scale from single-machine analyses to large high-performance computing environments. Snakemake addresses many limitations inherent to traditional bash pipelines, while providing a robust foundation for sustainable data analysis.

![alt text](snakemaketraits.png)

#### **Snakemake is designed to be sustainable, survive interruptions, and evolve with new data and processes**
- **Automation**: Snakemake automatically figures out which steps to run, in which order, based on declared inputs/outputsand the target files you ask for, instead of manually chaining commands.  
- **Scalability**: The same Snakefile can scale from a few samples on a laptop to hundreds on an HPC cluster by parallelizing independent rules and adjusting only the execution profile, not the workflow logic or code.
- **Portability**: Per-rule Conda or container definitions let Snakemake pin software and versions, so the exact sameworkflow can run reliably across different machines and compute environments.  
- **Readability**: Snakemake uses a Pythonic, rule-based syntax where each step is a small, named block (rule) with explicitinput, output, and commands, making complex pipelines easier to understand and review, even to complete beginners.  
- **Traceability**: Because every file is produced by a specific rule and command, Snakemake can show exactly how eachoutput was generated (DAG, rulegraph, logs), enabling you to trace results back through all intermediate steps.  
- **Documentation**: The combination of clear rule names, config files, environment specs, and built-in reporting (ex. `--report`) makes problems easy to diagnose within the workflow.  

## 2. RNA-seq & Typical Pipelines<a name="2"></a>
RNA sequencing (RNA-seq) is a widely used method for measuring gene expression by profiling the RNA molecules present in a biological sample. In practice, an RNA-seq experiment produces raw sequencing reads in FASTQ format. A standard workflow begins with quality assessment using tools such as FastQC or fastp, followed by optional trimming to remove adapters or low-quality bases. The cleaned reads are then processed either through traditional genome alignment (STAR, HISAT2, Bowtie2) or through faster pseudoalignment approaches (Salmon, Kallisto) that quantify transcript abundance without generating full alignments. Gene or transcript-level counts are subsequently derived using tools such as featureCounts or tximport, and these counts feed into statistical methods like DESeq2 for differential expression analysis. Outputs typically include normalized expression matrices, diagnostic plots, and multi-sample summaries, often consolidated through MultiQC.

<img src="inclassrnaseqexample.png" alt="alt text" width="200"/>

Although conceptually straightforward, RNA-seq analysis involves many interdependent steps and dozens of intermediate files, making manual execution fragile and difficult to scale. The workflow must also adapt to varying sample numbers, library types, reference genomes, and computational environments. These characteristics make RNA-seq an ideal case for workflow automation: a system like Snakemake provides structure, reproducibility, and scalability by formally defining how each step connects, ensuring that results are generated consistently.

## 4. Snakemake Basics: Rules, DAGs, and Wildcards <a name="4"></a>

Snakemake workflows are built around a simple but powerful idea: instead of explicitly writing out the order of operations, you describe *how files are created from other files*, and Snakemake automatically infers the correct execution order. This is achieved through rules, wildcards, and the construction of a Directed Acyclic Graph (DAG) that defines all necessary steps. 

### 4.0 Getting Started<a name="40"></a>

#### Installation: Conda
```bash
conda create -n snakemake -c conda-forge -c bioconda snakemake
conda activate snakemake
```

#### Installation: Mamba

mamba create -n snakemake snakemake -c conda-forge -c bioconda
mamba activate snakemake

---

An example minimal Snakemake project contains:

project/

│── Snakefile

│── config.yaml

│── envs/

│ └── rnaseq.yaml

│── scripts/

│ └── helper.py

│── data/

│ └── sample1_R1.fastq.gz

│── results/

---

#### Running Snakemake (local):
snakemake --cores 4

#### Running Snakemake on HPC:
snakemake --profile profiles/sge

#### Running Snakemake on SLURM:
snakemake --cluster “"sbatch -t {resources.time} -c {threads}" --jobs 50

### 4.1 Rules & `rule all`<a name="41"></a>

In Snakemake, **rules** define input and output files, rather than dictating explicit step-by-step execution. Each rule includes fields such as `input`, `output`, `params`, `threads`, `resources`, and a `shell` or `run` block that performs the actual operation. Most workflows also include a top-level **`rule all`** that declares the desired final outputs of the pipeline. Snakemake uses these targets as the endpoint, working backward to determine which intermediate files and rules must be executed. This makes workflows clean, declarative, and highly modular.

### 4.2 DAG Construction & Incremental Runs<a name="42"></a>

Snakemake uses the relationships between rule inputs and outputs to construct a **Directed Acyclic Graph (DAG)**, where 
each job is a node and edges represent dependencies. The DAG dictates execution order, enabling Snakemake to run independent jobs in parallel and skip any steps whose outputs are already up to date. Tools such as `--dag`, `--rulegraph`, and `--dry-run` make it easy to visualize or validate the workflow before executing it. For cases where output filenames or counts aren’t known until runtime, Snakemake offers **checkpoints**, which pause DAG creation, inspect dynamic outputs, and then expand the graph accordingly.

### 4.3 Wildcards<a name="43"></a>

**Wildcards** such as `{sample}` or `{chr}` allow a single rule to operate on multiple files without duplication. Snakemake automatically infers wildcard values based on matching file patterns. For example, if the rule expects an input `mapped_reads/{sample}.bam` and you have a file `mapped_reads/B.bam`, Snakemake assigns `{sample} = B`. Wildcards also work inside shell commands using `{wildcards.sample}`, enabling highly scalable pipelines that adapt seamlessly as sample lists grow. Together with `expand()`, wildcards make batch processing straightforward and maintainable.

### 4.4 Configs, Resources, and Execution Backends <a name="44"></a>

Workflows often use a `config.yaml` file to store paths, sample tables, and parameters, making Snakemake portable and easy to reuse across different datasets. Rules can specify resource needs such as threads, memory, and GPUs, and Snakemake schedules jobs efficiently to optimize compute usage. It supports a wide range of execution backends, from local execution to HPC schedulers, Kubernetes, Tibanna, and major cloud platforms without modifying the workflow itself. Reproducibility is strengthened further through per-rule Conda environments and container support, while utilities like `protected()` and `temp()` handle file safety and cleanup. Debugging is facilitated through tools like `--dry-run`, `--dag`, `--rulegraph`, and HTML reports generated via `--report`.

Together, these components form the foundation of Snakemake’s workflow model, enabling clean, scalable, and highly reproducible analyses across diverse computational environments.

## 5. Worked Example: RNA-seq Pipeline in Snakemake<a name="5"></a>
![alt text](exampledagworkflow.png)

### 5.0 Simplified Code Example: RNA-seq<a name="50"></a>
Below is a minimal RNA-seq Snakemake example:

```python
rule all:
    input:
        expand("results/quant/{sample}.csv", sample=config["samples"]),
        "results/multiqc/multiqc_report.html"

rule fastqc:
    input:  "data/{sample}.fastq.gz"
    output: "qc/fastqc/{sample}_fastqc.html"
    shell:
        "fastqc {input} --outdir qc/fastqc/"

rule align:
    input:  "data/{sample}.fastq.gz"
    output: "aligned/{sample}.bam"
    threads: 8
    params: index=config["star_index"]
    shell:
        "STAR --runThreadN {threads} --genomeDir {params.index} "
        "--readFilesIn {input} --outFileNamePrefix aligned/{wildcards.sample}."

rule quantify:
    input:  "aligned/{sample}.bam"
    output: "results/quant/{sample}.csv"
    params: gtf=config["gtf"]
    shell:
        "salmon quant -t {params.gtf} -l A -a {input} -o results/quant/{wildcards.sample}"
```
This very simple example shows the key components of Snakemake, emphasizing on connecting rules via declared inputs and outputs, wildcard generalization, and `rule all` defines final output. 


## 6. Limitations, Extensions, and Switching to Snakemake<a name="6"></a>
Even though Snakemake is powerful, it does come with certain limitations. Checkpoints, while extremely useful for handling dynamic or unknown inputs can be tricky to implement correctly. They require careful planning, and if the logic that determines downstream files is even slightly off, Snakemake may produce a malformed DAG or fail with confusing errors. Another fundamental limitation is that Snakemake workflows must be **acyclic**, meaning you cannot express iterative or recursive algorithms directly in the workflow graph. Any looped or cyclical process must be handled inside a script rather than through Snakemake’s rule structure.

Despite these limitations, Snakemake benefits from a very active community. There are extensive collections of ready-made wrappers (each bundling tools with pinned versions) and complete workflows that cover common analyses like RNA-seq, ChIP-seq, ATAC-seq, variant calling, metagenomics, and more. Many of these are maintained directly by the Snakemake team or by broader bioinformatics groups, which makes it much easier for new users to adopt working Snakemake models without writing pipelines from scratch. It’s also worth highlighting alternatives such as **Nextflow** (the other large workflow management tool designed specifically for computation biology), **CWL**, and **WDL/Cromwell**, which are widely used in other research communities. These workflow engines share many goals: adaptability, transparency, reproducibility, and sustainability, but differ in syntax (programming language), execution models, and strengths.

**Snakemake** stands out among some of the others because of its accessibility and smooth learning curve. Analyses of workflow complexity show that the majority of lines in a typical Snakefile fall into **complexity level 1**, simple rule declarations or keyword–colon structures, while only a small fraction reach **complexity level 7**, which corresponds to full Python expressions. In practice, this means that users can build functional, well-structured workflows without needing extensive programming experience, and only incorporate more advanced Python logic as their projects grow. This balance of simplicity and extensibility makes Snakemake particularly well-suited for teaching, onboarding new lab members, and maintaining long-term sustainable workflows.

<img src="snakemakecitationsovertime.png" alt="alt text" width="300"/>

## 7. Further Applications<a name="7"></a>

As mentioned previously, snakemake is useful beyond RNA-seq in applications across genomics, single-cell analysis, structural biology, clinical pipelines, teaching, and machine learning. Here are some additional Snakemake pipeline DAGs for alternative pipelines within computational biology:

| Domain | Snakemake Application | Key Features |
|-------|-----------------------------|--------------|
| Genomics | ATAC-seq, ChIP-seq, WGS, variant calling | Parallel alignment, large data handling |
| Single-cell | RNA-seq, ATAC, spatial datasets | Dynamic files, large sample sets |
| Structural Biology | AlphaFold batch runs | GPU scheduling, environment isolation |
| Clinical Pipelines | Germline/somatic variant pipelines | Reproducibility, audit trails |
| Machine Learning | Preprocessing & model training | Checkpoints, incremental builds |

## 8. References<a name="8"></a>
[1] Mölder, F., Jablonski, K. P., Letcher, B., Hall, M. B., Tomkins-Tinch, C. H., Sochat, V., *et al.* (2021). 
**Sustainable data analysis with Snakemake**. *F1000Research*, 10:33.  
    https://pmc.ncbi.nlm.nih.gov/articles/PMC8114187/

[2] **Snakemake Documentation**.  
    https://snakemake.readthedocs.io/en/stable/

[3] M. tuberculosis Bioinformatics Workshop. (2018). Welcome to bioinformatics workshop for M. tuberculosis genomics and phylogenomics at the Philippine Genome Center. https://mtbgenomicsworkshop.readthedocs.io/en/latest/index.html
