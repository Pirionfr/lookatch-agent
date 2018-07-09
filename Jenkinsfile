pipeline {
    agent { dockerfile true }
    stages {
        stage('Build') {
            steps {
                sh '''
                    mkdir -p ${GOPATH}/src/github.com/Pirionfr/
                    ln -s ${WORKSPACE} ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    cd ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    make all
                '''
            }
        }
        stage("build artifacts") {
            steps {
               sh '''#!/bin/bash -xe
                    make VERSION=${params.lkVersion} deb
                    make VERSION=${params.lkVersion} rpm
                    gpg --import ${gpgprivatekey}
                    chmod +x package/rpm-sign
                    ./package/rpm-sign ${gpgname} ${rpmpass} lookatch-agent*.rpm
                '''
            }
        }
    }
}