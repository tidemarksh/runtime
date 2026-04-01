package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	mode := "combined-goarch"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "combined-goarch":
		runCombinedGoarch()
	case "two-step-goarch":
		runTwoStepGoarch(false)
	case "two-step-goarch-explicit-env":
		runTwoStepGoarch(true)
	default:
		fmt.Printf("unknown mode=%s\n", mode)
		os.Exit(2)
	}
}

func compileCommand() *exec.Cmd {
	mustWriteImportCfg()
	return exec.Command(
		"/usr/local/go/pkg/tool/linux_riscv64/compile",
		"-o",
		"/tmp/goarch.a",
		"-trimpath",
		"/tmp=>",
		"-p",
		"internal/goarch",
		"-lang=go1.26",
		"-std",
		"-complete",
		"-buildid",
		"helper-buildid",
		"-goversion",
		"go1.26.1-X:nodwarf5",
		"-nolocalimports",
		"-importcfg",
		"/tmp/importcfg",
		"-pack",
		"/usr/local/go/src/internal/goarch/goarch.go",
		"/usr/local/go/src/internal/goarch/goarch_riscv64.go",
		"/usr/local/go/src/internal/goarch/zgoarch_riscv64.go",
	)
}

func mustWriteImportCfg() {
	if err := os.WriteFile("/tmp/importcfg", []byte("# import config\n"), 0o644); err != nil {
		fmt.Printf("importcfg_err=%T %v\n", err, err)
		os.Exit(80)
	}
}

func runCombinedGoarch() {
	fmt.Println("mode=combined-goarch")
	out, err := compileCommand().CombinedOutput()
	if err != nil {
		fmt.Printf("combined_output_err=%T %v\n", err, err)
		fmt.Printf("combined_output=%s", out)
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(40 + exitErr.ExitCode())
		}
		os.Exit(41)
	}
	fmt.Println("combined_output_err=<nil>")
	fmt.Printf("combined_output=%s", out)
}

func runTwoStepGoarch(explicitEnv bool) {
	mode := "two-step-goarch"
	if explicitEnv {
		mode = "two-step-goarch-explicit-env"
	}
	fmt.Printf("mode=%s\n", mode)
	fmt.Printf("parent_goroot=%q\n", os.Getenv("GOROOT"))
	fmt.Printf("parent_env_count=%d\n", len(os.Environ()))

	compileOut, err := compileCommand().CombinedOutput()
	if err != nil {
		fmt.Printf("compile_err=%T %v\n", err, err)
		fmt.Printf("compile_output=%s", compileOut)
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(90 + exitErr.ExitCode())
		}
		os.Exit(91)
	}
	fmt.Println("compile_err=<nil>")
	fmt.Printf("compile_output=%s", compileOut)

	buildid := exec.Command("/usr/local/go/bin/go", "tool", "buildid", "-w", "/tmp/goarch.a")
	if explicitEnv {
		buildid.Env = os.Environ()
	}
	buildidOut, err := buildid.CombinedOutput()
	if err != nil {
		fmt.Printf("buildid_err=%T %v\n", err, err)
		fmt.Printf("buildid_output=%s", buildidOut)
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(100 + exitErr.ExitCode())
		}
		os.Exit(101)
	}
	fmt.Println("buildid_err=<nil>")
	fmt.Printf("buildid_output=%s", buildidOut)
}
