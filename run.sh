make install && \
./nextflow run pipeline_mean.nf \
    -resume \
    -with-report \
    -with-trace \
    -with-timeline
