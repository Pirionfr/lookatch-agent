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
                        echo "$FILE" | gpg --batch --import
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
                sh '''#!/bin/bash -xe
                    make deb
                    make rpm
                    ./package/rpm-sign ${gpgname} ${rpmpass} lookatch-agent*.rpm
                '''
            }
        }
    }
}