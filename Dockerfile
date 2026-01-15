FROM quay.io/astronomer/astro-runtime:12.6.0

USER root
# 只安装程序运行必需的软件，不放任何账号、密码、地址
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && apt-get clean

USER astro