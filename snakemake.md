# Snakemake for RNA-seq  
###### BENG 183 – Sustainable Data Analysis with Workflow Managers  

---

1. [Overview & Learning Goals](#1)
2. [Why Workflow Automation? Why Snakemake?](#2)  
3. [RNA-seq & Typical Pipelines](#3)  
4. [Snakemake Basics: Rules, DAGs, and Wildcards](#4)  
   4.1. [Rules & the `rule all` Target](#41)  
   4.2. [DAG Construction & Incremental Runs](#42)  
   4.3. [Wildcards & `expand()`](#43)  
   4.4. [Configs, Resources, and Execution Backends](#44)  
5. [Reproducibility & Portability](#5)  
6. [Worked Example: RNA-seq Pipeline in Snakemake](#6)  
7. [Limitations, Gotchas, and Extensions](#7)  
8. [Alternative Workflow Managers](#8)  
9. [References & Further Reading](#9)  

---

## 1. Overview & Learning Goals<a name="1"></a>

![Snakemake RNA-seq Overview](img/snakemake_rnaseq_overview.png)

In class, we treated bulk RNA-seq as a linear pipeline:

> FASTQ → QC → trimming → alignment / pseudo-alignment → quantification → differential expression → plots

On slides this looks simple, but **real projects** quickly become messy:

- Dozens of samples, multiple conditions  
- Changing reference genomes or parameters  
- Different compute environments (laptop vs HPC vs cloud)  
- Hard-coded paths and copy-pasted Bash scripts  
- Constantly re-running steps manually when something changes  

Traditional, manually written pipelines (Bash scripts, Jupyter notebooks) **do not scale** well. They are hard to maintain, fragile, and difficult to reproduce.

**Workflow automation** and tools like **Snakemake** fix this by turning pipelines into *declarative* workflows: you describe the files that should exist and how they are produced, and the workflow manager figures out the order, parallelization, and reruns.

### Learning Goals

By the end of this lesson, you should be able to:

- Explain what a **workflow manager** does and why simple Bash pipelines break at scale  
- Describe **Snakemake’s core ideas**: file-based rules, DAGs, and automatic dependency resolution  
- Outline how an RNA-seq pipeline (FASTQ → counts → DE results) can be expressed in Snakemake  
- Understand key concepts: **rules, wildcards, `rule all`, configs, conda/containers, checkpoints**  
- Recognize **limitations** of Snakemake and know about major alternatives (Nextflow, CWL, WDL/Cromwell)  

---

## 2. Why Workflow Automation? Why Snakemake?<a name="2"></a>

### 2.1 What is workflow automation?

A **workflow** is a set of tasks with dependencies:

- Example: FastQC must run on raw FASTQ files before trimming  
- Trimming must happen before alignment  
- Alignment must finish before counting, and so on  

**Workflow automation** uses software to:

- Run tasks in the correct **order** based on their dependencies  
- Check which **inputs** and **outputs** exist  
- Run independent tasks **in parallel**  
- Resume from partial results without restarting everything  
- Capture the **structure** of the analysis so it can be rerun later  

Instead of you manually running 20 commands in your shell in exactly the right order, the workflow manager does it for you.

### 2.2 Why Snakemake?

**Snakemake** is a workflow management system with:

- A **Python-based** syntax  
- A focus on **reproducible** and **scalable** scientific workflows  
- Strong adoption in **bioinformatics** (but usable for any data analysis)  

Core design ideas:

- **Declarative**  
  - You describe how to produce outputs from inputs (rules), not a hand-written script ordering  
- **File-driven**  
  - Dependencies inferred from filenames and patterns  
- **DAG-based** (Directed Acyclic Graph)  
  - Jobs are nodes, file dependencies are edges  
- **Incremental**  
  - Only out-of-date or missing outputs are rebuilt  
- **Scalable & portable**  
  - Same Snakefile can run on a laptop, HPC cluster, or cloud, with different execution configs but no logic changes  

Compared to pure Bash/Jupyter:

- No manual sample loops in every script  
- No manual hand-tracking of which steps have completed  
- No giant “mega-script” that breaks if one line changes  

---

## 3. RNA-seq & Typical Pipelines<a name="3"></a>

![RNA-seq Pipeline](img/rnaseq_pipeline.png)

### 3.1 Biological context (very brief)

- Central dogma: **DNA → RNA → protein**
- RNA-seq measures **RNA abundance** (which genes are “on” and how strongly they are expressed)

### 3.2 Typical bulk RNA-seq computational pipeline

1. **Raw reads** (FASTQ) from the sequencer  
2. **Quality control**  
   - Tools: FastQC, fastp  
3. **Trimming/filtering**  
   - Tools: Trimmomatic, Cutadapt, fastp  
4. **Alignment** to a reference genome  
   - Tools: STAR, HISAT2, bowtie2  
5. **Pseudo-alignment** (alternative) to a reference transcriptome  
   - Tools: Salmon, Kallisto (directly estimate transcript abundance without full alignment)  
6. **Quantification**  
   - Tools: featureCounts, HTSeq, Salmon + tximport, etc.  
7. **Differential expression (DE)**  
   - Tools: DESeq2, edgeR, limma-voom  
8. **Visualization & summary**  
   - PCA, heatmaps, volcano plots, MultiQC, custom figures  

Snakemake can orchestrate *all* of these steps in a single, coherent workflow from raw FASTQs to final figures.

---

## 4. Snakemake Basics: Rules, DAGs, and Wildcards<a name="4"></a>

### 4.1 Rules & the `rule all` Target<a name="41"></a>

A **rule** in Snakemake describes one transformation:

- What it **needs** (inputs)  
- What it **produces** (outputs)  
- How to **run** it (shell command, script, or Python code)  

Example:

```python
rule fastqc:
    input:
        "data/{sample}.R1.fastq.gz",
        "data/{sample}.R2.fastq.gz"
    output:
        "qc/fastqc/{sample}_R1_fastqc.html",
        "qc/fastqc/{sample}_R2_fastqc.html"
    threads: 2
    shell:
        """
        fastqc -t {threads} {input} -o qc/fastqc
        """
