FROM ubuntu:18.04

RUN  apt-get update \
  && apt-get install -y wget gnupg
  
#install python 3.7 through Miniconda
WORKDIR /opt
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py37_4.8.3-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> /etc/bash.bashrc && \
    echo "conda activate base" >> /etc/bash.bashrc

ENV PATH /opt/conda/bin:$PATH

#install postgres11 (the specific version 11 is needed)
# add the repository to get version 11, # get the signing key and import it
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list 
RUN wget https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN apt-key add ACCC4CF8.asc

RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata

# fetch the metadata from the new repo
RUN apt-get update \
    && apt-get install -y postgresql-11 libpq-dev

ENV PATH /usr/lib/postgresql/11/bin:$PATH

#install node10
RUN apt -y install curl dirmngr apt-transport-https lsb-release ca-certificates \
    && curl -sL https://deb.nodesource.com/setup_10.x | bash \
    && apt update \
    && apt -y install gcc g++ make nodejs

#update npm
RUN npm cache clean --force
RUN npm update
RUN npm install -g npm@latest

#install ruby
RUN apt install -y ruby-full
RUN gem install compass

#install remaining dependencies
RUN apt-get install -y openjdk-11-jre-headless nginx graphviz redis git bsdtar 

#get elasticsearch 5.6 binary 
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.0.tar.gz \
    && tar -xzvf elasticsearch-5.6.0.tar.gz
ENV PATH /opt/elasticsearch-5.6.0/bin:$PATH

#install snovault from git repo
WORKDIR /opt
RUN git clone https://github.com/ENCODE-DCC/snovault.git
WORKDIR /opt/snovault
RUN conda install pip
RUN pip install -r requirements.txt
RUN make clean && buildout bootstrap && bin/buildout

#NOTE:SNOVAULT CAN'T BE RUN AS ROOT, BUT FIRST WE CHMOD /tmp and /var/log to be writable by our snovault-user for logging purposes
RUN useradd snovault-user
RUN chmod -R 777 /opt
RUN chmod -R 777 /tmp
RUN chmod -R 777 /var
RUN chmod -R 777 /run
WORKDIR /opt/snovault

#copy snovault start script 
COPY start_snovault.sh .
RUN chmod +x start_snovault.sh

#snovault runs on port 6543 by default 
EXPOSE 6543

USER snovault-user
CMD ./start_snovault.sh


