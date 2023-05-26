import sys

if __name__ == '__main__':
    filename = sys.argv[1]
    with open(filename) as f:
        for line in f:
            if 'mapped' in line and not 'primary' in line:
                percent_str = line.split("(")[1].split("%")[0]
                percent_val = float(percent_str)
                break

    if percent_val > 90:
        print("GOOD")
    else:
        print("BAD")