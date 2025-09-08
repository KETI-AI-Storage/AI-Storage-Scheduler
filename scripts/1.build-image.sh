registry="ketidevit2"
image_name="ai-storage-scheduler"
version="v0.0.1"
dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

#latest golang
# export PATH=$PATH:/usr/local/go/bin && \
# go mod init keti/$image_name
# go mod vendor
# go mod tidy

# build binary file
go build -o "$dir/../build/_output/bin/$image_name" -mod=vendor "$dir/../cmd/main.go"

# make image (Dockerfile must be in build/)
docker build -t $image_name:$version "$dir/../build"

# add tag
docker tag $image_name:$version $registry/$image_name:$version 

# login
docker login 

# push image
docker push $registry/$image_name:$version 