/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"github.com/IBM/multi-cluster-app-dispatcher/cmd/kar-controllers/app"
	"github.com/IBM/multi-cluster-app-dispatcher/cmd/kar-controllers/app/options"
	"github.com/spf13/pflag"
	"k8s.io/apiserver/pkg/util/flag"
	"os"
)


func main() {
	s := options.NewServerOption()
	s.AddFlags(pflag.CommandLine)

	flag.InitFlags()
	s.CheckOptionOrDie()

	if err := app.Run(s); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
