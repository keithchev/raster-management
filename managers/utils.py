
import subprocess


def shell(command=None, verbose=True):

    output = subprocess.run(
        command, 
        shell=True, 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True)

    if verbose:
        if output.stderr:
            print(output.stderr)
        if output.stdout:
            print(output.stdout)
            
    return output.stdout


def current_commit():

    return shell('git log', verbose=False).split(' ')[1].split('\n')[0][:7]