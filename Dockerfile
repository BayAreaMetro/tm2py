# docker build -t mypackage .
# docker run --rm -v "$PWD":/home/jovyan/work mypackage /bin/bash 
FROM jupyter/minimal-notebook

COPY ../requirements.txt /tmp/requirements.txt

# configure conda and install packages in one RUN to keep image tidy
RUN conda config --set show_channel_urls true && \
    conda config --set channel_priority strict && \
    conda config --prepend channels conda-forge && \
    conda update --yes -n base conda && \
    conda install --update-all --force-reinstall --yes --file /tmp/requirements.txt

#RUN rm -f -r -v /opt/conda/share/jupyter/kernels/python3 && \
#    python -m ipykernel install &&

COPY . /tmp/src
RUN pip install /tmp/src

RUN conda clean --all --yes && \
    conda info --all && \
    conda list && \
    jupyter kernelspec list && \
    ipython -c "import tm2py; print('Installed version: ', tm2py.__version__)"

# copy default jupyterlab settings, then set jupyter working directory to map to mounted volume
# COPY overrides.json /opt/conda/share/jupyter/lab/settings/
WORKDIR /home/jovyan/work

# set default command to launch when container is run
CMD ["jupyter", "lab", "--ip='0.0.0.0'", "--port=8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]