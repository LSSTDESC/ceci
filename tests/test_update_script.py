from ceci.update_pipelines_for_ceci_2 import scan_directory_and_update
import shutil
import os
import yaml

def test_update_script():
    # copy test files to temp directory
    os.makedirs("tests/update_test_files_tmp", exist_ok=True)
    shutil.copy("tests/update_test_files/config.yml", "tests/update_test_files_tmp/config.yml")
    shutil.copy("tests/update_test_files/pipeline1.yml", "tests/update_test_files_tmp/pipeline1.yml")
    shutil.copy("tests/update_test_files/pipeline2.yml", "tests/update_test_files_tmp/pipeline2.yml")
    os.chdir("tests/update_test_files_tmp")
    # run the scan process
    try:
        scan_directory_and_update(".")

        with open("config.yml") as f:
            config_txt = f.read()
            config = yaml.safe_load(config_txt)
        
        for info in config.values():
            assert "aliases" not in info

        with open("pipeline1.yml") as f:
            txt1 = f.read()
            pipeline1 = yaml.safe_load(txt1)

        with open("pipeline2.yml") as f:
            txt2 = f.read()
            pipeline2 = yaml.safe_load(txt2)
        
        assert pipeline1["config"] == "config.yml"
        assert pipeline2["config"] == "config.yml"
        assert pipeline1["stages"][0]["aliases"] == {"aaa": "aaa1", "bbb": "bbb1"}
        assert pipeline1["stages"][1]["aliases"] == {"bbb": "bbb1", "ccc": "ccc1"}
        assert pipeline2["stages"][0]["aliases"] == {"aaa": "aaa1", "bbb": "bbb1"}
        assert pipeline2["stages"][1]["aliases"] == {"bbb": "bbb1", "ccc": "ccc1"}
        assert "aliases" not in pipeline2["stages"][2]
        assert "# for debugging" in config_txt
        assert "# This comment should be preserved" in txt1
        assert "# So should this" in txt2
    finally:
        os.chdir("../..")
        shutil.rmtree("tests/update_test_files_tmp")
