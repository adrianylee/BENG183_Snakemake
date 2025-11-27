# Snakemake for RNA-seq  
###### BENG 183 – Sustainable Data Analysis with Workflow Managers  

---

1. [Introduction: Why Workflow Automation? Why Snakemake?](#1)  
2. [RNA-seq & Typical Pipelines](#2)  
3. [Snakemake Basics: Rules, DAGs, and Wildcards](#3)  
   3.1. [Rules & `rule all`](#31)  
   3.2. [DAG Construction & Incremental Runs](#32)  
   3.3. [Wildcards](#33)  
   3.4. [Configs, Resources, and Execution Backends](#34)  
4. [Reproducibility & Portability](#4)  
5. [Worked Example: RNA-seq Pipeline in Snakemake](#5)  
6. [Limitations and Extensions](#6)  
7. [Alternative Workflow Managers](#7)  
8. [References](#8)  

---

## 1. Introduction: Why Workflow Automation? Why Snakemake?<a name="1"></a>
Workflow automation means using software to coordinate and execute a series of tasks without constant human input. Instead of manually running one tool after another and tracking files, conditions, and paths by hand, an automated workflow system ensures that tasks are run in the correct order and using the right data. Snakemake is a popular workflow management tool for computational biology due to its simplicity, focus on reproducibility, and scalability. It solves many of the key issues that bash scripts tend to have. 

![alt text](https://)

#### **Snakemake is designed to be sustainable, surive interruptions, and evolve with new data and processes**
- **Automation**: Snakemake automatically figures out which steps to run, in which order, based on declared inputs/outputs and the target files you ask for, instead of manually chaining commands.  
- **Scalability**: The same Snakefile can scale from a few samples on a laptop to hundreds on an HPC cluster by parallelizing independent rules and adjusting only the execution profile, not the workflow logic.  
- **Portability**: Per-rule Conda or container definitions let Snakemake pin software and versions, so the exact same workflow can run reliably across different machines, OSes, and compute environments.  
- **Readability**: Snakemake uses a Pythonic, rule-based syntax where each step is a small, named block (rule) with explicit input, output, and commands, making complex pipelines easier to understand and review.  
- **Traceability**: Because every file is produced by a specific rule and command, Snakemake can show exactly how each output was generated (DAG, rulegraph, logs), enabling you to trace results back through all intermediate steps.  
- **Documentation**: The combination of clear rule names, config files, environment specs, and built-in reporting (e.g. `--report`) means the workflow itself serves as executable documentation of the entire analysis.  

## 2. RNA-seq & Typical Pipelines<a name="2"></a>


## 3. Snakemake Basics: Rules, DAGs, and Wildcards<a name="3"></a>


## 4. Reproducibility & Portability<a name="4"></a>


## 5. Worked Example: RNA-seq Pipeline in Snakemake<a name="5"></a>


## 6. Limitations and Extensions<a name="6"></a>


## 7. Alternative Workflow Managers<a name="7"></a>


## 8. References<a name="8"></a>
[1] Mölder, F., Jablonski, K. P., Letcher, B., Hall, M. B., Tomkins-Tinch, C. H., Sochat, V., *et al.* (2021). **Sustainable data analysis with Snakemake**. *F1000Research*, 10:33.  
    https://pmc.ncbi.nlm.nih.gov/articles/PMC8114187/

[2] **Snakemake Documentation**.  
    https://snakemake.readthedocs.io/en/stable/
