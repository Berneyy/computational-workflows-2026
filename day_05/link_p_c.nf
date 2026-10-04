#!/usr/bin/env nextflow

process SPLITLETTERS {
    input:
    val sample_in

    output:
    path "*.txt"

    script:
    def block_size = sample_in[0].block_size
    def input_str = sample_in[1][0]
    def out_name  = sample_in[1][1]
    """
    in_str='${input_str}'

    end=\$(( \${#in_str} - 1 ))
    for i in \$(seq 0 ${block_size} \${end}); do
        echo "\${in_str:\$i:${block_size}}" > ${out_name}_\$(( \$i/${block_size} )).txt
    done
    """
} 

process CONVERTTOUPPER {
    publishDir "results", mode: "copy"

    input:
    path filepath

    output:
    path filepath
    stdout

    script:
    def file = filepath
    """
    upper=\$(cat $file)
    echo "\${upper^^}"
    echo "\${upper^^}" > $file
    """
} 

workflow { 
    // 1. Read in the samplesheet (samplesheet_2.csv)  into a channel. The block_size will be the meta-map
    // 2. Create a process that splits the "in_str" into sizes with size block_size. The output will be a file for each block, named with the prefix as seen in the samplesheet_2
    // 4. Feed these files into a process that converts the strings to uppercase. The resulting strings should be written to stdout

    // read in samplesheet
    in_ch = channel.fromPath('samplesheet_2.csv', checkIfExists: true)
                   .splitCsv(header: true, sep:",")
                   .map { row ->
                            [['block_size': row.block_size], 
                            [row.input_str, row.out_name]]
                       }

    // split the input string into chunks
    file_ch = SPLITLETTERS(in_ch).flatten()

    // lets remove the metamap to make it easier for us, as we won't need it anymore

    // convert the chunks to uppercase and save the files to the results directory
    out_ch = CONVERTTOUPPER(file_ch)
    out_ch[1].view()


}