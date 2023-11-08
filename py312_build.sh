# sources:
# install local openssl version
# https://help.dreamhost.com/hc/en-us/articles/360001435926-Installing-OpenSSL-locally-under-your-username
# install local python version
# https://pages.github.nceas.ucsb.edu/NCEAS/Computing/local_install_python_on_a_server.html

python_version=3.12.0
openssl_version=3.1.4
pushd /tmp
	wget https://www.openssl.org/source/openssl-$openssl_version.tar.gz
	tar zxvf openssl-$openssl_version.tar.gz
	openssl_path=~/common/openssl-$openssl_version
	pushd openssl-$openssl_version
		./config --prefix=$openssl_path --openssldir=$openssl_path no-ssl2
		make
		make test
		make install
		echo "Add these lines to the .bashrc"
		echo '===================================='
		echo 'export PATH=$openssl_path/bin:$PATH'
		echo 'export LD_LIBRARY_PATH=$openssl_path/lib'
		echo 'export LC_ALL="en_US.UTF-8"'
		echo 'export LDFLAGS="-L $openssl_path/lib -Wl,-rpath,$openssl_path/lib"'
		echo '===================================='
		echo 'cd ~ # goto your home folder'
		echo '. ~/.bashrc # Update the .bashrc'
	popd
	rm -fr openssl-$openssl_version*
	
	wget https://www.python.org/ftp/python/$python_version/Python-$python_version.tgz
	tar -zxvf Python-$python_version.tgz

	python_path=~/common/$python_version
	pushd Python-$python_version
		mkdir -p $python_path
		./configure --prefix=$(realpath $python_path)
		make
		make install
	popd
	rm -fr Python-$python_version*
popd
