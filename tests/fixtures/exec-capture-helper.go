package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	mode := "combined"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "two-step-goarch" || mode == "two-step-goarch-explicit-env" {
		runTwoStepGoarch(mode == "two-step-goarch-explicit-env")
		return
	}
	cmd := compileCommand(mode)
	if mode == "combined-stdin" {
		cmd.Stdin = os.Stdin
	}
	fmt.Printf("mode=%s\n", mode)
	if mode == "output" || mode == "output-stdin" {
		if mode == "output-stdin" {
			cmd.Stdin = os.Stdin
		}
		out, err := cmd.Output()
		if err != nil {
			fmt.Printf("output_err=%T %v\n", err, err)
		} else {
			fmt.Println("output_err=<nil>")
		}
		fmt.Printf("output=%s", out)
		if err == nil {
			return
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(50 + exitErr.ExitCode())
		}
		os.Exit(51)
	}
	if mode == "separate" {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		fmt.Println("before_start")
		err := cmd.Start()
		if err != nil {
			fmt.Printf("start_err=%T %v\n", err, err)
			os.Exit(70)
		}
		fmt.Printf("after_start pid=%d\n", cmd.Process.Pid)
		fmt.Println("before_wait")
		err = cmd.Wait()
		fmt.Println("after_wait")
		if err != nil {
			fmt.Printf("run_err=%T %v\n", err, err)
		} else {
			fmt.Println("run_err=<nil>")
		}
		fmt.Printf("stdout=%s", stdout.Bytes())
		fmt.Printf("stderr=%s", stderr.Bytes())
		if err == nil {
			return
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(60 + exitErr.ExitCode())
		}
		os.Exit(61)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("combined_output_err=%T %v\n", err, err)
	} else {
		fmt.Println("combined_output_err=<nil>")
	}
	fmt.Printf("combined_output=%s", out)

	if err == nil {
		return
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		os.Exit(40 + exitErr.ExitCode())
	}
	os.Exit(41)
}

func compileCommand(mode string) *exec.Cmd {
	if mode == "combined-goarch" || mode == "separate-goarch" {
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
	return exec.Command("/usr/local/go/pkg/tool/linux_riscv64/compile", "-V=full")
}

func mustWriteImportCfg() {
	if err := os.WriteFile("/tmp/importcfg", []byte("# import config\n"), 0o644); err != nil {
		fmt.Printf("importcfg_err=%T %v\n", err, err)
		os.Exit(80)
	}
}

func runTwoStepGoarch(explicitEnv bool) {
	mode := "two-step-goarch"
	if explicitEnv {
		mode = "two-step-goarch-explicit-env"
	}
	fmt.Printf("mode=%s\n", mode)
	fmt.Printf("parent_goroot=%q\n", os.Getenv("GOROOT"))
	fmt.Printf("parent_env_count=%d\n", len(os.Environ()))
	compile := compileCommand("combined-goarch")
	compileOut, err := compile.CombinedOutput()
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
