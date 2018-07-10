pipeline {
    agent {
        dockerfile {
            args '-u root:root'
        }
    }
    stages {
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
                withCredentials([file(credentialsId: 'gpgprivatekey', variable: 'FILE')]) {
                   sh '''#!/bin/bash -xe
                        make deb
                        make rpm
                        cat
                        gpg --import $FILE
                        chmod +x package/rpm-sign
                        ./package/rpm-sign ${gpgname} ${rpmpass} lookatch-agent*.rpm
                    '''
                }
            }
        }
    }
}