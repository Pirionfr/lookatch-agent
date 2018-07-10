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
                        make deb
                        make rpm
                        gpg --list-keys ${gpgname}
                        ./package/rpm-sign ${gpgname} $GPGTOKEN lookatch-agent*.rpm
                    '''
                }
            }
        }
    }
}