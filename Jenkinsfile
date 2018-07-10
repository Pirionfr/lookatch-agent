pipeline {
    agent {
        dockerfile {
            args '-u root:root'
        }
    }
    stages {
         stage('Init') {
            steps {
                withCredentials([file(credentialsId: '${gpgprivatekey}', variable: 'FILE')]) {
                    sh '''#!/bin/bash -xe
                        export GPG_TTY=$(tty)
                        gpg --batch --import $FILE
                        chmod +x package/rpm-sign
                    '''
                }
            }
        }
        stage('Build') {
            steps {
                sh '''
                    mkdir -p ${GOPATH}/src/github.com/Pirionfr/
                    ln -s ${WORKSPACE} ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    cd ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    make release
                '''
            }
        }
        stage("build artifacts") {
            steps {
                withCredentials([string(credentialsId: '${rpmpass}', variable: 'GPGTOKEN')]) {
                    sh '''#!/bin/bash -xe
                        export GPG_TTY=$(tty)
                        echo "%_gpg_name C4F21B73" > ~/.rpmmacros
                        make deb
                        make rpm
                    '''
                }
            }
        }
        stage("deploy artifacts") {
            steps {
                withCredentials([string(credentialsId: '${apirpmpass}', variable: 'apipass')]) {
                    sh '''#!/bin/bash -xe

                    '''
                }
            }
        }
    }
}