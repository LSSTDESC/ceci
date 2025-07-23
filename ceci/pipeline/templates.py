import jinja2.compiler
import jinja2.meta
import sys

def read_and_apply_template(pipeline_config_filename, template_parameters=None):
        # YAML input file.
        # Load the text and then expand any environment variables
        with open(pipeline_config_filename) as config_file:
            raw_config_text = config_file.read()

        if template_parameters is None:
            template_parameters = {}            

        # Determine if the configuration file contains any jinja2 template variables
        # and check that the provided parameters match those required by the template
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        template = env.from_string(raw_config_text)

        # I assume this is internally repetitive somewhere,
        # but I couldn't see a way to do this cleanly just once.
        parsed_text = env.parse(raw_config_text)
        used_template_vars = jinja2.meta.find_undeclared_variables(parsed_text)

        # Check that all template parameters are used in the template
        unused = []
        for var in template_parameters:
            if var not in used_template_vars:
                unused.append(var)
        if unused:
            raise ValueError(f"These template parameters were specified but not used: {unused}")

        config_text = template.render(template_parameters)

        return config_text
