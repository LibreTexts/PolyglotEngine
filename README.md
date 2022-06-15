![LibreTexts](https://cdn.libretexts.net/DefaultImages/libretexts_logo_full_trans.png)
# Polyglot Engine
The Polyglot Engine provides infrastructure to load open access texts from LibreTexts libraries, pre-process and translate them, then save them to a designated path on another LibreTexts library.

As of the *alpha* release, it is designed to run on [Amazon Web Services](https://aws.amazon.com/) cloud infrastructure and use the [Amazon Translate](https://aws.amazon.com/translate) neural machine translation service.

## Architecture
The Polyglot Engine consists of three seperate modules or "cylinders":
1. **Ignition** validates requests and adds them to the Engine queue.
2. **StartTranslation** gathers content from the specified library, pre-processes it, then initiates the machine translation.
3. **ProcessTranslated** retrieves the translated content, reassembles the original text's structure, then saves it to the designated target library.

## Contact
For more information about the Polyglot Engine and how we use it, reach out to the LibreTexts team at info@libretexts.org.