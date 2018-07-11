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
                    make all
                '''
            }
        }
        stage("build artifacts") {
            steps {
                withCredentials([string(credentialsId: '${rpmpass}', variable: 'GPGTOKEN')]) {
                    sh '''#!/bin/bash -xe
                        make deb
                        make rpm
                    '''
                }
            }
        }
        stage("deploy artifacts") {
            steps {
                withCredentials([usernamePassword(credentialsId: '${repopassword}', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                    sh '''
                        filename="$(ls *.rpm | head -1)"
                        curl -u ${USERNAME}:${PASSWORD} --upload-file ${WORKSPACE}/${filename} ${repobaseurl}/centos/7/os/x86_64/${filename}

                        filename="$(ls *.deb | head -1)"
                        curl -u ${USERNAME}:${PASSWORD} -X POST -H "Content-Type: multipart/form-data" --data-binary "@${filename}" ${repobaseurl}/debian/
                    '''
                }
            }
        }
    }
}