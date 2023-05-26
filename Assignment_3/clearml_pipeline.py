from clearml.automation.controller import PipelineDecorator
from clearml import TaskTypes


def array_to_string(arr):
    return ' '.join(str(x) for x in arr)

def strsubexec(call):
    from subprocess import Popen, PIPE
    print(f"Calling: {call}")
    with Popen(call, shell=True, stdout=PIPE) as proc:
        res = proc.stdout.read()
    return res.decode('ascii')

def get_path(name):
    splited = strsubexec(f"whereis -b {name}").split()
    path = [a for a in splited if '/bin/' in a][0]
    print(f'Got path: {path}')
    return path

def my_exec(fn, call):
    return strsubexec(f"{get_path(fn)} {call}")

helper = [array_to_string, strsubexec, get_path, my_exec]


@PipelineDecorator.component(return_values=["chain"], task_type=TaskTypes.service)
def start():
    print("Starting pipeline")
    return 1

@PipelineDecorator.component(parents=['start'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.qc)
def qc_report(fastq_paths, out_dir, chain):
    my_exec('fastqc', f'{array_to_string(fastq_paths)} -o {out_dir}')
    return chain+1

@PipelineDecorator.component(parents=['start'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def fai_index(ungz_fasta_path, chain):
    my_exec('samtools', f"faidx {ungz_fasta_path}")
    return chain+1

@PipelineDecorator.component(parents=['start'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def minimap_index(ungzpath, mmi_path_name, chain):
    my_exec('minimap2', f'-d {mmi_path_name} {ungzpath}')
    return chain+1

@PipelineDecorator.component(parents=['minimap_index'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def minimap_align(mmi_path_name, fastq_paths, sam_path_name, chain):
    my_exec('minimap2', f'-a {mmi_path_name} {array_to_string(fastq_paths)} > {sam_path_name}')
    return chain+1

@PipelineDecorator.component(parents=['minimap_align'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def samtools_view(sam_path, bam_path, chain):
    my_exec('samtools', f'view -b {sam_path} > {bam_path}')
    return chain+1

@PipelineDecorator.component(parents=['samtools_view'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.qc)
def samtools_flagstat(bam_path, report_path_name, chain):
    my_exec('samtools', f'flagstat {bam_path} > {report_path_name}')
    return chain+1

@PipelineDecorator.component(parents=['samtools_flagstat'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.qc)
def evaluate_flagstat(report_path, chain):
    with open(report_path) as f:
        for line in f:
            if 'mapped' in line and not 'primary' in line:
                percent_str = line.split("(")[1].split("%")[0]
                percent_val = float(percent_str)
                break
    if percent_val > 90:
        print(f"GOOD {percent_val}")
        return 1
    else:
        print(f"BAD {percent_val}")
        return 0

@PipelineDecorator.component(parents=["evaluate_flagstat"], return_values=["chain"], task_type=TaskTypes.service)
def fault_exit(reason, chain):
    print(f"Exiting due to {reason}")
    return chain + 1

@PipelineDecorator.component(parents=['evaluate_flagstat'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def samtools_sort(bam_path, sorted_path_name, chain):
    print(my_exec('samtools', f'sort -o {sorted_path_name} {bam_path}'))
    return chain+1

@PipelineDecorator.component(parents=['samtools_sort'], return_values=["chain"], helper_functions=helper, task_type=TaskTypes.data_processing)
def freebayes(fasta_path, bam_sorted_path, vcf_path_name, chain):
    print(my_exec('freebayes', f'-f {fasta_path} {bam_sorted_path} > {vcf_path_name}'))
    return chain+1

@PipelineDecorator.pipeline(name='my_pipeline', project='bioinformatics', version='0.0.1')
def executing_pipeline(fastq_paths, fasta_path, mmi_path_name, sam_path_name, bam_path_name, bam_sorted_path_name, flagstat_report_path_name, fastqc_report_path, vcf_path_name):
    ungz_path = fasta_path[:-3]
    chain = start()
    chain1 = qc_report(fastq_paths, out_dir=fastqc_report_path, chain=chain)
    chain2 = fai_index(ungz_path, chain)
    chain = minimap_index(ungz_path, mmi_path_name, chain)
    chain = minimap_align(mmi_path_name, fastq_paths, sam_path_name, chain)
    chain = samtools_view(sam_path_name, bam_path_name, chain=chain)
    chain = samtools_flagstat(bam_path_name, flagstat_report_path_name, chain=chain)
    res = evaluate_flagstat(flagstat_report_path_name, chain=chain)
    if res:
        chain = samtools_sort(bam_path_name, bam_sorted_path_name, chain=res)
        chain = freebayes(ungz_path, bam_sorted_path_name, vcf_path_name, chain=chain)
    else:
        chain = fault_exit('bad alignment values', res)


if __name__ == '__main__':
    PipelineDecorator.run_locally()
    executing_pipeline(
        fastq_paths=['./biofiles/input.fastq.gz'],
        fasta_path='./biofiles/input.fna.gz',
        mmi_path_name='./biofiles/input.mmi',
        sam_path_name='./output.sam',
        bam_path_name='./output.bam',
        bam_sorted_path_name='./output.sorted.bam',
        flagstat_report_path_name='./flagstat.txt',
        fastqc_report_path='./biofiles',
        vcf_path_name='./result.vcf'
    )
